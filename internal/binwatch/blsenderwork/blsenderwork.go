package blsenderwork

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"text/template"

	"binwatch/api/v1alpha2"
	"binwatch/internal/cache"
	"binwatch/internal/connectors"
	"binwatch/internal/logger"
	"binwatch/internal/pools"
	"binwatch/internal/tmpl"
	"binwatch/internal/utils"
)

const (
	componentName = "BinlogSenderWorker"
)

type BLSenderWorkT struct {
	log logger.LoggerT
	cfg *v1alpha2.ConfigT

	rePool *pools.RowEventPoolT
	cach   cache.CacheI
	conns  map[string]connectors.ConnectorI
	routs  []routeT

	shardEnabled bool
	shardCount   uint64
	shardIndex   uint64
	shardKeyTmpl *template.Template
}

type routeT struct {
	name string
	conn string
	ops  []string
	dbt  string
	tmpl *template.Template
}

func NewBinlogSenderWork(cfg *v1alpha2.ConfigT, rePool *pools.RowEventPoolT, cach cache.CacheI) (w *BLSenderWorkT, err error) {
	w = &BLSenderWorkT{
		log: logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg: cfg,

		rePool: rePool,
		conns:  make(map[string]connectors.ConnectorI),
		cach:   cach,

		shardEnabled: cfg.Sharding.Enabled,
		shardCount:   cfg.Sharding.Count,
		shardIndex:   cfg.Sharding.Index,
	}

	if w.shardEnabled {
		if w.shardCount == 0 {
			err = fmt.Errorf("sharding enabled but 'sharding.count' is zero")
			return w, err
		}
		if w.shardIndex >= w.shardCount {
			err = fmt.Errorf("sharding 'index' (%d) must be lower than 'count' (%d)", w.shardIndex, w.shardCount)
			return w, err
		}
		if cfg.Sharding.KeyTemplate != "" {
			w.shardKeyTmpl, err = tmpl.NewTemplate("sharding-key", cfg.Sharding.KeyTemplate)
			if err != nil {
				err = fmt.Errorf("error parsing 'sharding.keyTemplate': %w", err)
				return w, err
			}
		}
	}

	for _, connv := range cfg.Connectors {
		w.conns[connv.Name], err = connectors.NewConnector(connv)
		if err != nil {
			err = fmt.Errorf("error creating '%s' connector: %w", connv.Name, err)
			return w, err
		}
	}

	for _, rtv := range cfg.Routes {
		rt := routeT{
			name: rtv.Name,
			conn: rtv.Connector,
			ops:  rtv.Operations,
			dbt:  rtv.DBTable,
		}

		if _, ok := w.conns[rt.conn]; !ok {
			err = fmt.Errorf("error creating '%s' route: no '%s' connector in connector list", rtv.Name, rt.conn)
			return w, err
		}

		rt.tmpl, err = tmpl.NewTemplate(rt.name, rtv.Template)
		if err != nil {
			err = fmt.Errorf("error creating '%s' route: %w", rtv.Name, err)
			return w, err
		}

		w.routs = append(w.routs, rt)
	}

	return w, err
}

// shouldProcess returns true if the given event belongs to this shard.
// When sharding is disabled it always returns true.
//
// If a KeyTemplate is configured, it is rendered against the event and the
// resulting bytes are hashed (FNV-1a 64) to derive the bucket. This keeps
// related events (e.g. all updates to the same row PK) on the same shard.
//
// If no KeyTemplate is set, BinlogPosition is used as the bucket key, which
// distributes load evenly but does not guarantee same-row affinity.
func (w *BLSenderWorkT) shouldProcess(item *pools.RowEventItemT) bool {
	if !w.shardEnabled {
		return true
	}

	var bucket uint64
	if w.shardKeyTmpl != nil {
		buf := new(bytes.Buffer)
		if err := w.shardKeyTmpl.Execute(buf, item); err != nil {
			// On template error: fail-open (process), and log. Dropping
			// silently would risk losing events; processing on every shard
			// would duplicate. We pick "this shard handles it" so at least
			// one shard sends it.
			extra := utils.GetBasicLogExtraFields(componentName)
			w.log.Error("error rendering sharding.keyTemplate, falling back to BinlogPosition", extra, err, false)
			bucket = item.Log.BinlogPosition
		} else {
			h := fnv.New64a()
			_, _ = h.Write(buf.Bytes())
			bucket = h.Sum64()
		}
	} else {
		bucket = item.Log.BinlogPosition
	}

	return bucket%w.shardCount == w.shardIndex
}

func (w *BLSenderWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	extra := utils.GetBasicLogExtraFields(componentName)

	runWorker := true
	for runWorker {
		select {
		case <-ctx.Done():
			{
				runWorker = false
				w.log.Info("execution cancelled", extra)
			}
		default:
			{
				var err error
				var item *pools.RowEventItemT
				extra.Del("event")

				item, err = w.rePool.Get(ctx)
				if err != nil {
					if err != context.Canceled {
						w.log.Error("error getting item from pool", extra, err, w.cfg.Server.StopInError)
					}
					continue
				}
				extra.Set("event", item)

				if !w.shouldProcess(item) {
					continue
				}

				for ri := range w.routs {
					if slices.Contains(w.routs[ri].ops, item.Data.Operation) &&
						fmt.Sprintf("%s.%s", item.Data.Database, item.Data.Table) == w.routs[ri].dbt {

						buffer := new(bytes.Buffer)
						err = w.routs[ri].tmpl.Execute(buffer, item)
						if err != nil {
							w.log.Error("error executing template in pool item", extra, err, w.cfg.Server.StopInError)
							break
						}

						err = w.conns[w.routs[ri].conn].Send(buffer.Bytes())
						if err != nil {
							w.log.Error("error sending data to connector", extra, err, w.cfg.Server.StopInError)
							break
						}
					}
				}
				if err != nil {
					continue
				}

				if w.cfg.Server.Cache.Enabled {
					err = w.cach.Store(cache.BinlogLocation{
						File:     item.Log.BinlogFile,
						Position: uint32(item.Log.BinlogPosition),
					})
					if err != nil {
						w.log.Error("error saving current location in cache", extra, err, w.cfg.Server.StopInError)
						continue
					}
				}

				w.log.Info("success sending event to connector", extra)
			}
		}
	}
}
