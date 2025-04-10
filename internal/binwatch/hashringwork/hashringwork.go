package hashringwork

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"binwatch/api/v1alpha1"
	"binwatch/internal/hashring"
	"binwatch/internal/logger"
)

type HashRingWorkT struct {
	log     logger.LoggerT
	cfg     *v1alpha1.ConfigSpec
	hr      *hashring.HashRing
	hrReady *atomic.Bool
}

func NewHashRingWork(cfg *v1alpha1.ConfigSpec, hr *hashring.HashRing, hrReady *atomic.Bool) (w *HashRingWorkT, err error) {
	w = &HashRingWorkT{
		log:     logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg:     cfg,
		hr:      hr,
		hrReady: hrReady,
	}
	return w, err
}

func (w *HashRingWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	if !w.cfg.Hashring.Enabled {
		return
	}
	extra := logger.ExtraFieldsT{"component": "HashringWorker"}

	currentNodes := []string{}
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
				w.log.Debug("waiting to next sync", extra)
				time.Sleep(2 * time.Second)

				var err error
				var nodes []string
				nodes, err = w.getNodeList()
				if err != nil {
					w.log.Error("error in get nodes", extra, err)
					continue
				}

				slices.Sort(nodes)
				if slices.Equal(currentNodes, nodes) {
					w.log.Debug("no updates in hashring", extra)
					continue
				}
				currentNodes = nodes

				w.hrReady.Store(false)
				w.hr.Replace(currentNodes)
				w.hrReady.Store(true)

				extra.Set("currentHashring", w.hr.String())
				w.log.Info("hashring updated", extra)
			}
		}
	}
}
