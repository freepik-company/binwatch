package hashringwork

import (
	"fmt"
	"net"
	"net/http"
	"reflect"
)

func (w *HashRingWorkT) getNodeList() (nodes []string, err error) {
	if !reflect.ValueOf(w.cfg.Hashring.StaticRingDiscovery).IsZero() {
		for _, hv := range w.cfg.Hashring.StaticRingDiscovery.Hosts {
			var resp *http.Response
			resp, err = http.Get(fmt.Sprintf("http://%s/healthz", hv))
			if err != nil {
				err = fmt.Errorf("error in get healthz from node with '%s' host: %w", hv, err)
				return nodes, err
			}
			if resp.StatusCode == 200 {
				if sid := resp.Header.Get("X-Server-ID"); sid != "" {
					nodes = append(nodes, sid)
				}
			}
		}
	} else if !reflect.ValueOf(w.cfg.Hashring.DnsRingDiscovery).IsZero() {
		discoveredIps, err := net.LookupIP(w.cfg.Hashring.DnsRingDiscovery.Domain)
		if err != nil {
			err = fmt.Errorf("error discovering '%s' DNS hosts: %w", w.cfg.Hashring.DnsRingDiscovery.Domain, err)
			return nodes, err
		}

		for _, ipv := range discoveredIps {
			host := ipv.String() + ":" + w.cfg.Hashring.APIPort

			var resp *http.Response
			resp, err = http.Get(fmt.Sprintf("http://%s/healthz", host))
			if err != nil {
				err = fmt.Errorf("error in get healthz from node with '%s' host: %w", host, err)
				return nodes, err
			}
			if resp.StatusCode == 200 {
				if sid := resp.Header.Get("X-Server-ID"); sid != "" {
					nodes = append(nodes, sid)
				}
			}
		}
	} else {
		err = fmt.Errorf("empty hashring configuration, unable to gethashring nodes")
	}

	return nodes, err
}
