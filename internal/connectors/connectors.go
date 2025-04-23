package connectors

import (
	"binwatch/api/v1alpha2"
	"fmt"
)

const (
	TypeWebhook      = "webhook"
	TypeGooglePubsub = "google_pubsub"
)

type ConnectorI interface {
	Send([]byte) error
}

func NewConnector(cfg v1alpha2.ConnectorT) (conn ConnectorI, err error) {
	switch cfg.Type {
	case TypeWebhook:
		{
			conn, err = newWebhook(cfg)
		}
	case TypeGooglePubsub:
		{
			conn, err = newGooglePubsub(cfg)
		}
	default:
		{
			err = fmt.Errorf("unsuported '%s' connector type", cfg.Type)
		}
	}
	return conn, err
}
