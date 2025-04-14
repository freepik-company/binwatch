package connectors

import (
	"binwatch/api/v1alpha2"
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

type GooglePubsubT struct {
	name string
	ctx  context.Context
	cli  *pubsub.Client

	topicID string
}

func newGooglePubsub(cfg v1alpha2.ConnectorT) (c *GooglePubsubT, err error) {
	c = &GooglePubsubT{
		name: cfg.Name,
		ctx:  context.Background(),
	}

	c.cli, err = pubsub.NewClient(c.ctx, cfg.Pubsub.ProjectID)
	
	return c, err
}

func (c *GooglePubsubT) Send(dataBytes []byte) (err error) {
	result := c.cli.Topic(c.topicID).Publish(c.ctx, &pubsub.Message{
		Data: dataBytes,
	})

	var id string
	id, err = result.Get(c.ctx)
	if err != nil {
		err = fmt.Errorf("error publishing '%s' message in google pubsub: %w", id, err)
	}
	return err
}
