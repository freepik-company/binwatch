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

func Send(ctx v1alpha1.Context, jsonData []byte) {

	logger := ctx.Logger

	logger.Debug("Sending webhook", zap.String("data", string(jsonData)))
	// Create the HTTP client
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: ctx.Config.Connectors.WebHook.TlsSkipVerify,
			},
		},
	}

	// Create the request with the configured verb and URL
	httpRequest, err := http.NewRequest(ctx.Config.Connectors.WebHook.Method, ctx.Config.Connectors.WebHook.URL, nil)
	if err != nil {
		logger.Error("Error creating HTTP Request for webhook integration", zap.Error(err))
	}

	// Add headers to the request if set
	httpRequest.Header.Set("Content-Type", "application/json")
	for headerKey, headerValue := range ctx.Config.Connectors.WebHook.Headers {
		httpRequest.Header.Set(headerKey, headerValue)
	}

	// Add authentication if set for the webhook
	if ctx.Config.Connectors.WebHook.Credentials.Username != "" && ctx.Config.Connectors.WebHook.Credentials.Password != "" {
		httpRequest.SetBasicAuth(ctx.Config.Connectors.WebHook.Credentials.Username, ctx.Config.Connectors.WebHook.Credentials.Password)
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
	parsedMessage, err := template.EvaluateTemplate(ctx.Config.Connectors.Data, templateInjectedObject)
	if err != nil {
		logger.Error("Error evaluating template for webhook integration", zap.Error(err))
	}

	// Add data to the payload of the request
	payload := []byte(parsedMessage)
	httpRequest.Body = io.NopCloser(bytes.NewBuffer(payload))

	// Send HTTP request to the webhook
	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		logger.Error("Error sending HTTP request for webhook integration", zap.Error(err))
	}

	defer httpResponse.Body.Close()

	logger.Debug("Webhook sent successfully", zap.String("data", string(jsonData)))

}
