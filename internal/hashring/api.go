package hashring

import (
	"binwatch/api/v1alpha1"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"net/http"
)

func (h *HashRing) runHashRingAPI(app *v1alpha1.Application) {

	// Start the API server
	http.HandleFunc("/health", h.hashRingHealthHandler)
	http.HandleFunc("/position", func(w http.ResponseWriter, r *http.Request) {
		h.hashRingBinLogPositionHandler(w, r, app)
	})

	// Start the server
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", app.Config.Hashring.APIPort), nil)
		if err != nil {
			app.Logger.Fatal("Error starting hashring API server", zap.Error(err))
		}
	}()
}

func (h *HashRing) hashRingHealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *HashRing) hashRingBinLogPositionHandler(w http.ResponseWriter, r *http.Request, app *v1alpha1.Application) {

	response := map[string]interface{}{
		"file":     app.BinLogFile,
		"position": app.BinLogPosition,
	}

	// Convertir a JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Error generando JSON", http.StatusInternalServerError)
		return
	}

	// Write the response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}
