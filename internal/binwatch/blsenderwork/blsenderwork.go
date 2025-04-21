package blsenderwork

import (
	"bytes"
	"context"
	"fmt"
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
}

type routeT struct {
	name string
	conn string
	ops  []string
	tmpl *template.Template
}

func NewBinlogSenderWork(cfg *v1alpha2.ConfigT, rePool *pools.RowEventPoolT, cach cache.CacheI) (w *BLSenderWorkT, err error) {
	w = &BLSenderWorkT{
		log: logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg: cfg,

		rePool: rePool,
		conns:  make(map[string]connectors.ConnectorI),
		cach:   cach,
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

				for ri := range w.routs {
					if slices.Contains(w.routs[ri].ops, item.Data.Operation) {
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
