package binlogwork

import (
	"context"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"binwatch/api/v1alpha1"
)

type BinlogWorkT struct {
	cfg   *v1alpha1.ConfigSpec
	ready *atomic.Bool
}

func NewBinLogWork(cfg *v1alpha1.ConfigSpec, ready *atomic.Bool) (w *BinlogWorkT, err error) {
	w = &BinlogWorkT{
		cfg:   cfg,
		ready: ready,
	}
	return w, err
}

func (w *BinlogWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	// TODO: add propper logger

	if reflect.ValueOf(w.cfg.Sources.MySQL).IsZero() {
		log.Fatalf("ERROR No connector configuration found")
	}

	runWorker := true
	for runWorker {
		select {
		case <-ctx.Done():
			{
				runWorker = false
				log.Printf("INFO execution cancelled")
			}
		default:
			{
				log.Printf("DEBUG waiting to next binlog sync...")
				time.Sleep(2 * time.Second)

				// wait to hashring be ready
				if !w.ready.Load() {
					log.Printf("DEBUG hashring is not ready")
					continue
				}

				// mysql.Sync(&app, hr)
			}
		}
	}
}
