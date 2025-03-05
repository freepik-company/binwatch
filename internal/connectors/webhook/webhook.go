package webhook

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/template"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"go.uber.org/zap"
	"io"
	"net/http"
)

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
