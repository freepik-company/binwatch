package hashringwork

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
)

func (w *HashRingWorkT) getNodeList() (nodes []string, err error) {
	if !reflect.ValueOf(w.cfg.Hashring.StaticRingDiscovery).IsZero() {
		for _, hv := range w.cfg.Hashring.StaticRingDiscovery.Hosts {
			var resp *http.Response
			resp, err = http.Get(fmt.Sprintf("http://%s/server", hv))
			if err != nil {
				err = fmt.Errorf("error in get info from node with '%s' host: %w", hv, err)
				return nodes, err
			}

			if resp.StatusCode == 200 {
				var bodyBytes []byte
				if bodyBytes, err = io.ReadAll(resp.Body); err != nil {
					err = fmt.Errorf("error in download body bytes in node with '%s' host: %w", hv, err)
					return nodes, err
				}

				body := map[string]any{}
				if err = json.Unmarshal(bodyBytes, &body); err != nil {
					err = fmt.Errorf("error in parse body response from node with '%s' host: %w", hv, err)
					return nodes, err
				}

				if _, ok := body["host"]; !ok {
					err = fmt.Errorf("no host defined in body from node with '%s' host: %w", hv, err)
					return nodes, err
				}
				if _, ok := body["port"]; !ok {
					err = fmt.Errorf("no port defined in body from node with '%s' host: %w", hv, err)
					return nodes, err
				}

				nodes = append(nodes, fmt.Sprintf("%s:%s", body["host"], body["port"]))
			}
		}
		nodes = append(nodes, fmt.Sprintf("%s:%d", w.cfg.Server.Host, w.cfg.Server.Port))
	} else if !reflect.ValueOf(w.cfg.Hashring.DnsRingDiscovery).IsZero() {
		discoveredIps, err := net.LookupIP(w.cfg.Hashring.DnsRingDiscovery.Domain)
		if err != nil {
			err = fmt.Errorf("error discovering '%s' DNS hosts: %w", w.cfg.Hashring.DnsRingDiscovery.Domain, err)
			return nodes, err
		}

		for _, ipv := range discoveredIps {
			host := fmt.Sprintf("%s:%d", ipv.String(), w.cfg.Server.Port)
			var resp *http.Response
			resp, err = http.Get(fmt.Sprintf("http://%s/server", host))
			if err != nil {
				err = fmt.Errorf("error in get info from node with '%s' host: %w", host, err)
				return nodes, err
			}

			if resp.StatusCode == 200 {
				var bodyBytes []byte
				if bodyBytes, err = io.ReadAll(resp.Body); err != nil {
					err = fmt.Errorf("error in download body bytes in node with '%s' host: %w", host, err)
					return nodes, err
				}

				body := map[string]any{}
				if err = json.Unmarshal(bodyBytes, &body); err != nil {
					err = fmt.Errorf("error in parse body response from node with '%s' host: %w", host, err)
					return nodes, err
				}

				if _, ok := body["host"]; !ok {
					err = fmt.Errorf("no host defined in body from node with '%s' host: %w", host, err)
					return nodes, err
				}
				if _, ok := body["port"]; !ok {
					err = fmt.Errorf("no port defined in body from node with '%s' host: %w", host, err)
					return nodes, err
				}

				nodes = append(nodes, fmt.Sprintf("%s:%s", body["host"], body["port"]))
			}
		}
	} else {
		err = fmt.Errorf("empty hashring configuration, unable to gethashring nodes")
	}

	return nodes, err
}
