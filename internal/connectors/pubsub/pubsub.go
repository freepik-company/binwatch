package pubsub

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/template"
	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

func Send(ctx v1alpha1.Context, jsonData []byte) {

	logger := ctx.Logger

	logger.Debug("Sending message to pubsub", zap.String("data", string(jsonData)))

	// Create the PubSub client
	pubsubClient, err := pubsub.NewClient(ctx.Context, ctx.Config.Connectors.PubSub.ProjectID)
	if err != nil {
		logger.Error("Error creating PubSub client", zap.Error(err))
	}
	defer pubsubClient.Close()

	// Get the topic
	topic := pubsubClient.Topic(ctx.Config.Connectors.PubSub.TopicID)

	// Add row data to the template injected
	templateInjectedObject := map[string]interface{}{}
	templateInjectedObject["rowJSON"] = jsonData

	// Evaluate the data template with the injected object
	parsedMessage, err := template.EvaluateTemplate(ctx.Config.Connectors.Data, templateInjectedObject)
	if err != nil {
		logger.Error("Error evaluating template for webhook integration", zap.Error(err))
	}

	// Publish the message
	result := topic.Publish(ctx.Context, &pubsub.Message{
		Data: []byte(parsedMessage),
	})

	// Wait for confirmation
	id, err := result.Get(ctx.Context)
	if err != nil {
		logger.Error("Error publishing message to pubsub", zap.Error(err))
	}

	logger.Debug("Message published to pubsub", zap.String("topic", ctx.Config.Connectors.PubSub.TopicID),
		zap.String("project", ctx.Config.Connectors.PubSub.ProjectID), zap.String("id", id))

}
