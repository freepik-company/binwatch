package hashringwork

import (
	"context"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"binwatch/api/v1alpha1"
	"binwatch/internal/hashring"
)

type HashRingWorkT struct {
	cfg   *v1alpha1.ConfigSpec
	ready *atomic.Bool

	hr *hashring.HashRing
}

func NewHashRing(cfg *v1alpha1.ConfigSpec, ready *atomic.Bool) (w *HashRingWorkT, err error) {
	w = &HashRingWorkT{
		cfg:   cfg,
		ready: ready,
	}
	w.hr = hashring.NewHashRing(1)
	return w, err
}

func (w *HashRingWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()

	// TODO: add propper logger
	currentNodes := []string{}
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
				log.Printf("DEBUG waiting to next hashring sync...")
				time.Sleep(2 * time.Second)

				var err error
				var nodes []string
				nodes, err = w.getNodeList()
				if err != nil {
					log.Printf("ERROR error in get nodes: %s", err.Error())
					continue
				}

				if len(currentNodes) == len(nodes) {
					slices.Sort(nodes)
					updated := false
					for ni := range nodes {
						if currentNodes[ni] != nodes[ni] {
							updated = true
						}
					}
					if !updated {
						log.Printf("INFO no updates in hashring")
						continue
					}
				} else {
					currentNodes = nodes
				}

				w.ready.Store(false)
				w.hr.Replace(currentNodes)
				w.ready.Store(true)

				log.Printf("INFO current hashring: %s", w.hr.String())
			}
		}
	}
}
