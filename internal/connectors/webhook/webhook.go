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

package webhook

import (
	//
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"

	//
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/template"
)

// Send sends a message to a webhook
func Send(app v1alpha1.Application, templateData string, jsonData []byte) {

	logger := app.Logger

	logger.Debug("Sending webhook", zap.String("data", string(jsonData)))
	
	// Create the HTTP client
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: app.Config.Connectors.WebHook.TlsSkipVerify,
			},
		},
	}

	// Create the request with the configured verb and URL
	httpRequest, err := http.NewRequest(app.Config.Connectors.WebHook.Method, app.Config.Connectors.WebHook.URL, nil)
	if err != nil {
		logger.Error("Error creating HTTP Request for webhook integration", zap.Error(err))
		return
	}

	// Add headers to the request if set
	httpRequest.Header.Set("Content-Type", "application/json")
	for headerKey, headerValue := range app.Config.Connectors.WebHook.Headers {
		httpRequest.Header.Set(headerKey, headerValue)
	}

	// Add authentication if set for the webhook
	if app.Config.Connectors.WebHook.Credentials.Username != "" && app.Config.Connectors.WebHook.Credentials.Password != "" {
		httpRequest.SetBasicAuth(app.Config.Connectors.WebHook.Credentials.Username, app.Config.Connectors.WebHook.Credentials.Password)
	}

	// Parse jsonData to a map
	data := map[string]interface{}{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		logger.Error("Error parsing JSON data for webhook integration", zap.Error(err))
		return
	}

	// Add row data to the template injected
	templateInjectedObject := map[string]interface{}{}
	templateInjectedObject["data"] = data

	// Evaluate the data template with the injected object
	parsedMessage, err := template.EvaluateTemplate(templateData, templateInjectedObject)
	if err != nil {
		logger.Error("Error evaluating template for webhook integration", zap.Error(err))
		return
	}

	// Add data to the payload of the request
	payload := []byte(parsedMessage)
	httpRequest.Body = io.NopCloser(bytes.NewBuffer(payload))

	// Send HTTP request to the webhook
	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		logger.Error("Error sending HTTP request for webhook integration", zap.Error(err))
		return
	}

	defer httpResponse.Body.Close()

	logger.Debug("Webhook sent successfully", zap.String("data", string(jsonData)))

}
