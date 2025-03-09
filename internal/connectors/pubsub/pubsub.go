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

package pubsub

import (
	//
	"encoding/json"
	"fmt"

	//
	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/template"
)

// Send sends a message to a PubSub topic
func Send(app *v1alpha1.Application, templateData string, jsonData []byte) (err error) {

	logger := app.Logger

	logger.Debug("Sending message to pubsub", zap.String("data", string(jsonData)))

	// Create the PubSub client
	pubsubClient, err := pubsub.NewClient(app.Context, app.Config.Connectors.PubSub.ProjectID)
	if err != nil {
		return fmt.Errorf("error creating PubSub client: %v", err)
	}
	defer pubsubClient.Close()

	// Parse jsonData to a map
	data := map[string]interface{}{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return fmt.Errorf("error parsing JSON data for webhook integration: %v", err)
	}

	// Get the topic
	topic := pubsubClient.Topic(app.Config.Connectors.PubSub.TopicID)

	// Add row data to the template injected
	templateInjectedObject := map[string]interface{}{}
	templateInjectedObject["data"] = data

	// Evaluate the data template with the injected object
	parsedMessage, err := template.EvaluateTemplate(templateData, templateInjectedObject)
	if err != nil {
		return fmt.Errorf("error evaluating template for webhook integration: %v", err)
	}

	// Publish the message
	result := topic.Publish(app.Context, &pubsub.Message{
		Data: []byte(parsedMessage),
	})

	// Wait for confirmation
	id, err := result.Get(app.Context)
	if err != nil {
		return fmt.Errorf("error publishing message to pubsub: %v", err)
	}

	logger.Debug("Message published to pubsub", zap.String("topic", app.Config.Connectors.PubSub.TopicID),
		zap.String("project", app.Config.Connectors.PubSub.ProjectID), zap.String("id", id))

	return nil

}
