package serverapi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"binwatch/api/v1alpha1"
)

type ServerAPIT struct {
	cfg   *v1alpha1.ConfigSpec
	ready *atomic.Bool

	server *http.Server
}

func NewBinWatchApi(cfg *v1alpha1.ConfigSpec, ready *atomic.Bool) (a *ServerAPIT, err error) {
	a = &ServerAPIT{
		cfg:   cfg,
		ready: ready,
	}

	mux := http.NewServeMux()

	// Endpoints
	mux.HandleFunc("/healthz", a.getHealthz)

	a.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%s", "0.0.0.0", a.cfg.Hashring.APIPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	return a, err
}

func (a *ServerAPIT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()

	go func() {
		<-ctx.Done()

		log.Printf("INFO execution cancelled")
		if err := a.server.Shutdown(context.Background()); err != nil {
			log.Printf("ERROR shutdown api: %s", err.Error())
		}
	}()

	// TODO: add propper logger
	if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("ERROR stop api: %s", err.Error())
	}
}

func (a *ServerAPIT) getHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("KO"))
	}

	w.Header().Set("X-Server-ID", a.cfg.ServerId)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
