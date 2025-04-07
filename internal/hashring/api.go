/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hashring

import (
	//
	"encoding/json"
	"fmt"
	"net/http"

	//
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
)

// runHashRingAPI starts the API server for the hashring
func (h *HashRing) runHashRingAPI(app *v1alpha1.Application) {

	// Define the API routes
	// Health check
	http.HandleFunc("/health", h.hashRingHealthHandler)
	// Binlog position
	http.HandleFunc("/position", func(w http.ResponseWriter, r *http.Request) {
		h.hashRingBinLogPositionHandler(w, r, app)
	})

	// Start the server
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%s", app.Config.Hashring.APIPort), nil)
		if err != nil {
			app.Logger.Fatal("Error starting hashring API server", zap.Error(err))
		}
	}()
}

// hashRingHealthHandler is the handler for the health check
func (h *HashRing) hashRingHealthHandler(w http.ResponseWriter, r *http.Request) {

	// Healthcheck
	if h.runHealthCheck() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("NOK"))
	}
}

// hashRingBinLogPositionHandler is the handler for the binlog position
func (h *HashRing) hashRingBinLogPositionHandler(w http.ResponseWriter, r *http.Request, app *v1alpha1.Application) {

	// Get the binlog position
	binLogFile := app.BinLogFile
	binLogPosition := app.BinLogPosition

	// Check if the server is rolling back and return this position to the rest of the servers
	if app.RollBackPosition != 0 && app.RollBackFile != "" {
		binLogFile = app.RollBackFile
		binLogPosition = app.RollBackPosition
	}

	// Create the response
	response := map[string]interface{}{
		"file":     binLogFile,
		"position": binLogPosition,
	}

	// Convert to JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Error generating JSON", http.StatusInternalServerError)
		return
	}

	// Write the response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}

// runHealthCheck runs the health check
func (h *HashRing) runHealthCheck() bool {
	return true
}
