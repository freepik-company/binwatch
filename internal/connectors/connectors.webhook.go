package connectors

import (
	"binwatch/api/v1alpha2"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net/http"
)

type WebhookT struct {
	name string
	cli  *http.Client

	method  string
	url     string
	headers map[string]string
	user    string
	pass    string
}

func newWebhook(cfg v1alpha2.ConnectorT) (c *WebhookT, err error) {
	c = &WebhookT{
		name: cfg.Name,

		method:  cfg.Webhook.Method,
		url:     cfg.Webhook.URL,
		headers: make(map[string]string),
		user:    cfg.Webhook.Credentials.Username,
		pass:    cfg.Webhook.Credentials.Password,
	}
	maps.Copy(c.headers, cfg.Webhook.Headers)

	c.cli = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: cfg.Webhook.TlsSkipVerify,
			},
		},
	}

	return c, err
}

func (c *WebhookT) Send(dataBytes []byte) (err error) {
	var req *http.Request
	req, err = http.NewRequest(c.method, c.url, io.NopCloser(bytes.NewBuffer(dataBytes)))
	if err != nil {
		err = fmt.Errorf("error creating webhook request: %w", err)
		return err
	}

	for hk, hv := range c.headers {
		req.Header.Set(hk, hv)
	}

	if req.Header.Get("Authorization") == "" && c.user != "" && c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}

	var resp *http.Response
	resp, err = c.cli.Do(req)
	if err != nil {
		err = fmt.Errorf("error executing request: %w", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err = fmt.Errorf("error in response, get '%d' code", resp.StatusCode)
	}

	return err
}
