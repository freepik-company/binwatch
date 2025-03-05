package hashring

import (
	"binwatch/api/v1alpha1"
	"go.uber.org/zap"
	"net"
	"reflect"

	"slices"
	"time"
)

// TODO
func (h *HashRing) SyncWorker(app *v1alpha1.Application, syncTime time.Duration) {

	if reflect.ValueOf(app.Config.Hashring).IsZero() {
		app.Logger.Info("No hashring configuration found")
		return
	}

	if !reflect.ValueOf(app.Config.Hashring.StaticRingDiscovery).IsZero() && !reflect.ValueOf(app.Config.Hashring.DnsRingDiscovery).IsZero() {
		app.Logger.Fatal("Just select one discovery method, please")
		return
	}

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
