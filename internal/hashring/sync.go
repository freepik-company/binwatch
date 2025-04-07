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
	"fmt"
	"net"
	"net/http"
	"reflect"
	"slices"
	"time"

	//
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
)

// SyncWorker is a worker that syncs the hashring
func (h *HashRing) SyncWorker(app *v1alpha1.Application, syncTime time.Duration) (err error) {

	// Check if hashring configuration is present. If not, just run alone
	if reflect.ValueOf(app.Config.Hashring).IsZero() {
		app.Logger.Info("No hashring configuration found, running in standalone mode")
		return
	}

	// Check if static and dns discovery are both present. If so, just run alone and shows error.
	if !reflect.ValueOf(app.Config.Hashring.StaticRingDiscovery).IsZero() && !reflect.ValueOf(app.Config.Hashring.DnsRingDiscovery).IsZero() {
		app.Logger.Fatal("Just select one discovery method, please. Running in standalone mode.")
		return
	}

	// Run API server for hashring communication and healthchecks
	h.runHashRingAPI(app)

	// Main loop to sync hashring and binlog positions
	for {

		tmpHostPool := []string{}
		hostPool := []string{}

		// Static mode
		// In static mode, hosts are defined as IP:PORT in the configuration file
		if !reflect.ValueOf(app.Config.Hashring.StaticRingDiscovery).IsZero() {
			for _, backend := range app.Config.Hashring.StaticRingDiscovery.Hosts {
				tmpHostPool = append(tmpHostPool, backend)
			}

		}

		// Dns autodiscover mode
		// In dns autodiscover mode, hosts are discovered by looking up the domain name, and then we append the port
		if !reflect.ValueOf(app.Config.Hashring.DnsRingDiscovery).IsZero() {

			discoveredIps, err := net.LookupIP(app.Config.Hashring.DnsRingDiscovery.Domain)
			if err != nil {
				app.Logger.Error(fmt.Sprintf("Error looking up domain %s",
					app.Config.Hashring.DnsRingDiscovery.Domain), zap.Error(err))
			}

			for _, discoveredIp := range discoveredIps {
				tmpHostPool = append(tmpHostPool, fmt.Sprintf("%s:%s", discoveredIp.String(), app.Config.Hashring.DnsRingDiscovery.Port))
			}
		}

		// After getting the temporal hosts pool, we perform a healthcheck on each host to determine if is candidate to be added to the hashring
		hClient := http.Client{}
		hClient.Timeout = time.Duration(1) * time.Second
		for _, backend := range tmpHostPool {

			// If it's the same host, skip the healthcheck
			if backend == app.Config.ServerId {
				hostPool = append(hostPool, backend)
				continue
			}

			// Execute healthcheck to http://<HOST+IP>/health
			resp, err := hClient.Get(fmt.Sprintf("http://%s/health", backend))
			if err == nil && resp.StatusCode == 200 {
				hostname := resp.Header.Get("X-Server-ID")
				if hostname == "" {
					app.Logger.Error(fmt.Sprintf("No hostname returned from healthcheck on host %s", backend))
					continue
				}
				hostPool = append(hostPool, hostname)
			}

			if err != nil || resp.StatusCode != 200 {
				app.Logger.Error(fmt.Sprintf("Unable to perform healthcheck on host %s with http", backend), zap.Error(err))
			}

		}

		// Get the current server list and compare with the new one. Add or remove servers from the hashring
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
			app.Logger.Info(fmt.Sprintf("Adding server %s to hashring", server))
			h.AddServer(server)
		}

		// If there are servers to delete, we remove them from the hashring and update the binlog position to the lowest
		// node removed position in the hashring
		for _, server := range deleteServersList {
			app.Logger.Info(fmt.Sprintf("Removing server %s from hashring", server))
			h.RemoveServer(server)
		}

		time.Sleep(syncTime)
	}
}
