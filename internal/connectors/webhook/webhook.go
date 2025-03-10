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
	"fmt"
	"io"
	"net/http"

	//
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/template"
)

// Send sends a message to a webhook
func Send(app *v1alpha1.Application, templateData string, wh v1alpha1.WebHookConfig, jsonData []byte) (err error) {

	logger := app.Logger

	logger.Debug("Sending webhook", zap.String("data", string(jsonData)))

	// Create the HTTP client
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: wh.TlsSkipVerify,
			},
		},
	}

	// Create the request with the configured verb and URL
	httpRequest, err := http.NewRequest(wh.Method, wh.URL, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP Request for webhook integration: %v", err)
	}

	// Add headers to the request if set
	httpRequest.Header.Set("Content-Type", "application/json")
	for headerKey, headerValue := range wh.Headers {
		httpRequest.Header.Set(headerKey, headerValue)
	}

	// Add authentication if set for the webhook
	if wh.Credentials.Username != "" && wh.Credentials.Password != "" {
		httpRequest.SetBasicAuth(wh.Credentials.Username, wh.Credentials.Password)
	}

	// Parse jsonData to a map
	data := map[string]interface{}{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return fmt.Errorf("error parsing JSON data for webhook integration: %v", err)
	}

	// Add row data to the template injected
	templateInjectedObject := map[string]interface{}{}
	templateInjectedObject["data"] = data

	// Evaluate the data template with the injected object
	parsedMessage, err := template.EvaluateTemplate(templateData, templateInjectedObject)
	if err != nil {
		return fmt.Errorf("error evaluating template for webhook integration: %v", err)
	}

	// Add data to the payload of the request
	payload := []byte(parsedMessage)
	httpRequest.Body = io.NopCloser(bytes.NewBuffer(payload))

	// Send HTTP request to the webhook
	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("error sending HTTP request for webhook integration: %v", err)
	}

	defer httpResponse.Body.Close()

	logger.Debug("Webhook sent successfully", zap.String("data", string(jsonData)))

	return nil
}
