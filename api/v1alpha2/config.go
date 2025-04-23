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

package v1alpha2

import "time"

// ConfigT
type ConfigT struct {
	Logger     LoggerT      `yaml:"logger"`
	Server     ServerT      `yaml:"server"`
	Source     SourceT      `yaml:"source"`
	Connectors []ConnectorT `yaml:"connectors"`
	Routes     []RouteT     `yaml:"routes"`
}

/*
 *  Logger configuration
 */

// LoggerT
type LoggerT struct {
	Level string `yaml:"level"`
}

/*
 *  Server configuration
 */

// ServerT
type ServerT struct {
	ID          string       `yaml:"id"`
	Host        string       `yaml:"host"`
	Port        uint32       `yaml:"port"`
	StopInError bool         `yaml:"stopInError"`
	Pool        ServerPoolT  `yaml:"pool"`
	Cache       ServerCacheT `yaml:"cache"`
}

type ServerPoolT struct {
	Size      uint32 `yaml:"size"`
	ItemByRow bool   `yaml:"itemByRow"`
}

// ServerCacheT
type ServerCacheT struct {
	Enabled bool              `yaml:"enabled"`
	Type    string            `yaml:"type"` // values: local|redis
	Local   ServerCacheLocalT `yaml:"local"`
	Redis   ServerCacheRedisT `yaml:"redis"`
}

// ServerCacheLocalT
type ServerCacheLocalT struct {
	Path string `yaml:"path"`
}

// ServerCacheRedisT
type ServerCacheRedisT struct {
	Host     string `yaml:"host"`
	Port     uint32 `yaml:"port"`
	Password string `yaml:"password"`
}

/*
 *  Source configuration
 */

// SourceT
type SourceT struct {
	Flavor   string              `yaml:"flavor"`
	ServerID uint32              `yaml:"serverID"`
	Host     string              `yaml:"host"`
	Port     uint32              `yaml:"port"`
	User     string              `yaml:"user"`
	Password string              `yaml:"password"`
	DBTables map[string][]string `yaml:"dbTables"`

	ReadTimeout     time.Duration `yaml:"readTimeout"`
	HeartbeatPeriod time.Duration `yaml:"heartbeatPeriod"`
	StartLocation   LocationT     `yaml:"startLocation"`
}

// LocationT
type LocationT struct {
	File     string `yaml:"file"`
	Position uint32 `yaml:"position"`
}

/*
 *  Connectors configuration
 */

// ConnectorT
type ConnectorT struct {
	Name    string            `yaml:"name"`
	Type    string            `yaml:"type"`
	Pubsub  ConnectorPubsubT  `yaml:"pubsub"`
	Webhook ConnectorWebhookT `yaml:"webhook"`
}

// ConnectorPubsubT
type ConnectorPubsubT struct {
	ProjectID string `yaml:"projectID"`
	TopicID   string `yaml:"topicID"`
}

// ConnectorWebhookT
type ConnectorWebhookT struct {
	URL           string                       `yaml:"url"`
	Method        string                       `yaml:"method"`
	Headers       map[string]string            `yaml:"headers"`
	Credentials   ConnectorWebhookCredentialsT `yaml:"credentials"`
	TlsSkipVerify bool                         `yaml:"tlsSkipVerify"`
}

// ConnectorWebhookCredentialsT
type ConnectorWebhookCredentialsT struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

/*
 *  Routes configuration
 */

type RouteT struct {
	Name       string   `yaml:"name"`
	Operations []string `yaml:"operations"`
	Connector  string   `yaml:"connector"`
	Template   string   `yaml:"template"`
	DBTable    string   `yaml:"dbTable"`
}
