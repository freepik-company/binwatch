package pubsub

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/template"
	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

func Send(app v1alpha1.Application, templateData string, jsonData []byte) {

	logger := app.Logger

	logger.Debug("Sending message to pubsub", zap.String("data", string(jsonData)))

	// Create the PubSub client
	pubsubClient, err := pubsub.NewClient(app.Context, app.Config.Connectors.PubSub.ProjectID)
	if err != nil {
		logger.Error("Error creating PubSub client", zap.Error(err))
		return
	}
	defer pubsubClient.Close()

	// Get the topic
	topic := pubsubClient.Topic(app.Config.Connectors.PubSub.TopicID)

	// Add row data to the template injected
	templateInjectedObject := map[string]interface{}{}
	templateInjectedObject["rowJSON"] = jsonData

	// Evaluate the data template with the injected object
	parsedMessage, err := template.EvaluateTemplate(templateData, templateInjectedObject)
	if err != nil {
		logger.Error("Error evaluating template for webhook integration", zap.Error(err))
		return
	}

	// Publish the message
	result := topic.Publish(app.Context, &pubsub.Message{
		Data: []byte(parsedMessage),
	})

	// Wait for confirmation
	id, err := result.Get(app.Context)
	if err != nil {
		logger.Error("Error publishing message to pubsub", zap.Error(err))
		return
	}

	logger.Debug("Message published to pubsub", zap.String("topic", app.Config.Connectors.PubSub.TopicID),
		zap.String("project", app.Config.Connectors.PubSub.ProjectID), zap.String("id", id))

}
