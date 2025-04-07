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
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		h.hashRingHealthHandler(w, r, app)
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
func (h *HashRing) hashRingHealthHandler(w http.ResponseWriter, r *http.Request, app *v1alpha1.Application) {

	// Healthcheck
	if h.runHealthCheck() {
		w.Header().Set("X-Server-ID", app.Config.ServerId)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("NOK"))
	}
}

// runHealthCheck runs the health check
func (h *HashRing) runHealthCheck() bool {
	return true
}
