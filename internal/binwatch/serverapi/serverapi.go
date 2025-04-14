package serverapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"binwatch/api/v1alpha2"
	"binwatch/internal/logger"
	"binwatch/internal/utils"
)

const (
	componentName = "API"

	EndpointPathHealthz     = "/healthz"
	EndpointPathAPIV1Server = "/api/v1/server"
)

type ServerAPIT struct {
	log logger.LoggerT
	cfg *v1alpha2.ConfigT

	server *http.Server
}

func NewBinWatchApi(cfg *v1alpha2.ConfigT) (a *ServerAPIT, err error) {
	a = &ServerAPIT{
		log: logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg: cfg,
	}

	mux := http.NewServeMux()

	// Endpoints
	mux.HandleFunc(EndpointPathHealthz, a.getHealthz)
	mux.HandleFunc(EndpointPathAPIV1Server, a.getServer)

	a.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", a.cfg.Server.Host, a.cfg.Server.Port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	return a, err
}

func (a *ServerAPIT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	extra := utils.GetBasicLogExtraFields(componentName)

	go func() {
		<-ctx.Done()

		a.log.Info("execution cancelled", extra)
		if err := a.server.Shutdown(ctx); err != nil {
			a.log.Error("error in shutdown execution", extra, err)
		}
	}()

	extra.Set("serve", a.server.Addr)
	a.log.Info("init API", extra)
	if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		a.log.Error("error in API execution", extra, err)
	}
}

func (a *ServerAPIT) getHealthz(w http.ResponseWriter, r *http.Request) {
	data := []byte("KO")
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write(data)
		return
	}

	data = []byte("OK")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (a *ServerAPIT) getServer(w http.ResponseWriter, r *http.Request) {
	extra := utils.GetBasicLogExtraFields(componentName)

	data := []byte("KO")
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write(data)
		return
	}

	var err error
	server := map[string]any{
		"id":   a.cfg.Server.ID,
		"host": a.cfg.Server.Host,
		"port": a.cfg.Server.Port,
	}
	data, err = json.Marshal(server)
	if err != nil {
		data = []byte("KO")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write(data)

		a.log.Error("unable to encode server json", extra, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
