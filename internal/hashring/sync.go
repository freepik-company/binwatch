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
	//
	"net"
	"net/http"
	"reflect"
	"slices"
	"time"

	//
	"github.com/go-ping/ping"
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

		tmpHostPool := []string{}
		hostPool := []string{}

		// STATIC ---
		if !reflect.ValueOf(app.Config.Hashring.StaticRingDiscovery).IsZero() {
			for _, backend := range app.Config.Hashring.StaticRingDiscovery.Hosts {
				tmpHostPool = append(tmpHostPool, backend)
			}

		}

		// DNS ---
		if !reflect.ValueOf(app.Config.Hashring.DnsRingDiscovery).IsZero() {

			discoveredIps, err := net.LookupIP(app.Config.Hashring.DnsRingDiscovery.Domain)
			if err != nil {
				app.Logger.Error("error looking up", zap.String("domain", app.Config.Hashring.DnsRingDiscovery.Domain), zap.Error(err))
			}

			for _, discoveredIp := range discoveredIps {
				tmpHostPool = append(tmpHostPool, discoveredIp.String())
			}
		}

		// HealthCheck ---
		if !reflect.ValueOf(app.Config.Hashring.HealthCheck).IsZero() {

			// For ICMP health check
			if app.Config.Hashring.HealthCheck.Type == "icmp" {
				for _, backend := range tmpHostPool {
					if pingCheck(app, backend, app.Config.Hashring.HealthCheck.Timeout) {
						hostPool = append(hostPool, backend)
					} else {
						app.Logger.Error("unable to perform healthcheck on host with icmp", zap.String("peer", backend))
					}
				}
			}

			// For HTTP health check
			if app.Config.Hashring.HealthCheck.Type == "http" {
				hClient := http.Client{}
				hClient.Timeout = time.Duration(app.Config.Hashring.HealthCheck.Timeout) * time.Second
				for _, backend := range tmpHostPool {
					resp, err := hClient.Get(fmt.Sprintf("http://%s%s", backend, app.Config.Hashring.HealthCheck.Path))
					if err == nil && resp.StatusCode == 200 {
						hostPool = append(hostPool, backend)
						break
					}

					if err != nil {
						app.Logger.Error("unable to perform healthcheck on host with http", zap.String("peer", backend), zap.Error(err))
						continue
					}
				}
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
			app.Logger.Info("adding server to hashring", zap.String("server_added", server))
			h.AddServer(server)
		}

		for _, server := range deleteServersList {
			app.Logger.Info("removing server from hashring", zap.String("server_removed", server))
			h.RemoveServer(server)
		}

		if len(h.GetServerList()) == 0 {
			app.Logger.Info("empty hashring", zap.String("nodes", h.String()))
		}

		time.Sleep(syncTime)
	}
}

// pingCheck function to check if a host is reachable via icmp
func pingCheck(app *v1alpha1.Application, host string, timeout int) bool {

	// Create a new pinger
	pinger, err := ping.NewPinger(host)
	if err != nil {
		app.Logger.Error("Error creating ping object", zap.Error(err))
		return false
	}

	// Set the pinger configuration
	pinger.Count = 1
	pinger.Timeout = time.Duration(timeout) * time.Second
	pinger.SetPrivileged(true)

	// Run the pinger
	err = pinger.Run()
	if err != nil {
		app.Logger.Error("Error running ping", zap.Error(err))
		return false
	}

	stats := pinger.Statistics()
	return stats.PacketsRecv > 0
}
