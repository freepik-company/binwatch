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
	"net"
	"reflect"
	"slices"
	"time"

	//
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
)

// SyncWorker TODO
func (h *HashRing) SyncWorker(app *v1alpha1.Application, syncTime time.Duration) {

	// Check if hashring configuration is present. If not, just run alone
	if reflect.ValueOf(app.Config.Hashring).IsZero() {
		app.Logger.Info("No hashring configuration found")
		return
	}

	// Check if static and dns discovery are both present. If so, just run alone and shows error.
	if !reflect.ValueOf(app.Config.Hashring.StaticRingDiscovery).IsZero() && !reflect.ValueOf(app.Config.Hashring.DnsRingDiscovery).IsZero() {
		app.Logger.Fatal("Just select one discovery method, please")
		return
	}

	// Main loop to sync hashring
	for {

		hostPool := []string{}

		// STATIC ---
		if !reflect.ValueOf(app.Config.Hashring.StaticRingDiscovery).IsZero() {
			for _, backend := range app.Config.Hashring.StaticRingDiscovery.Hosts {
				hostPool = append(hostPool, backend)
			}

		}

		// DNS ---
		if !reflect.ValueOf(app.Config.Hashring.DnsRingDiscovery).IsZero() {

			app.Logger.Info("syncing hashring with DNS")

			discoveredIps, err := net.LookupIP(app.Config.Hashring.DnsRingDiscovery.Domain)
			if err != nil {
				app.Logger.Error("error looking up", zap.String("domain", app.Config.Hashring.DnsRingDiscovery.Domain), zap.Error(err))
			}

			for _, discoveredIp := range discoveredIps {
				hostPool = append(hostPool, discoveredIp.String())
			}
		}

		currentServerList := h.GetServerList()

		deleteServersList := []string{}
		for _, server := range currentServerList {
			if !slices.Contains(hostPool, server) {
				deleteServersList = append(deleteServersList, server)
			}
		}

		appendServersList := []string{}
		for _, server := range hostPool {
			if !slices.Contains(currentServerList, server) {
				appendServersList = append(appendServersList, server)
			}
		}

		for _, server := range appendServersList {
			h.AddServer(server)
		}

		for _, server := range deleteServersList {
			h.RemoveServer(server)
		}

		if len(currentServerList) == 0 {
			app.Logger.Info("empty hashring", zap.String("nodes", h.String()))
		}

		time.Sleep(syncTime)
	}
}
