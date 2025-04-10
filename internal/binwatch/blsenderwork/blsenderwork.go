package blsenderwork

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"binwatch/api/v1alpha1"
	"binwatch/internal/hashring"
	"binwatch/internal/logger"
	"binwatch/internal/pools"
)

type BLSenderWorkT struct {
	log     logger.LoggerT
	cfg     *v1alpha1.ConfigSpec
	cqpool  *pools.ConnectorsQueuePoolT
	hr      *hashring.HashRing
	hrReady *atomic.Bool
}

func NewBinlogSenderWork(cfg *v1alpha1.ConfigSpec, cqpool *pools.ConnectorsQueuePoolT, hr *hashring.HashRing, hrReady *atomic.Bool) (w *BLSenderWorkT, err error) {
	w = &BLSenderWorkT{
		log:     logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg:     cfg,
		cqpool:  cqpool,
		hr:      hr,
		hrReady: hrReady,
	}
	return w, err
}

func (w *BLSenderWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	extra := logger.ExtraFieldsT{"component": "BinlogSenderWorker"}

	if reflect.ValueOf(w.cfg.Sources.MySQL).IsZero() {
		w.log.Fatal("error in sender initialization", extra, fmt.Errorf("no connector configuration found"))
	}

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

				// wait to hashring be ready
				if w.cfg.Hashring.Enabled && !w.hrReady.Load() {
					w.log.Info("hashring is not ready", extra)
					continue
				}

				// mysql.Sync(&app, hr)
			}
		}
	}
}
