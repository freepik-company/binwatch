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

package v1alpha1

// ConfigSpec
type ConfigSpec struct {
	Logger     LoggerConfig     `yaml:"logger"`
	Sources    SourcesConfig    `yaml:"sources"`
	Connectors ConnectorsConfig `yaml:"connectors"`
	Hashring   HashringConfig   `yaml:"hashring"`
	ServerId   string           `yaml:"server_id"`
}

// HashringConfig
type HashringConfig struct {
	SyncWorkerTimeMs    int                    `yaml:"sync_worker_time_ms"`
	APIPort             int                    `yaml:"api_port"`
	DnsRingDiscovery    DnsDiscoveryRingConfig `yaml:"dns_ring_discovery"`
	StaticRingDiscovery StaticRingConfig       `yaml:"static_ring_discovery"`
}

// StaticRingConfig
type StaticRingConfig struct {
	Hosts []string `yaml:"hosts"`
}

// DnsDiscoveryConfig
type DnsDiscoveryRingConfig struct {
	Domain string `yaml:"domain"`
	Port   int    `yaml:"port"`
}

// LoggerConfig
type LoggerConfig struct {
	Encoding string `yaml:"encoding"`
	Level    string `yaml:"level"`
}

// SourcesConfig
type SourcesConfig struct {
	MySQL MySQLConfig `yaml:"mysql"`
}

// MySQLConfig
type MySQLConfig struct {
	SyncTimeoutMs   int           `yaml:"sync_timeout_ms"`
	Host            string        `yaml:"host"`
	Port            uint16        `yaml:"port"`
	User            string        `yaml:"user"`
	Password        string        `yaml:"password"`
	ServerID        uint32        `yaml:"server_id"`
	Flavor          string        `yaml:"flavor"`
	ReadTimeout     int           `yaml:"read_timeout"`
	HeartbeatPeriod int           `yaml:"heartbeat_period"`
	FilterTables    []FilterTable `yaml:"filter_tables"`
}

// FilterTable
type FilterTable struct {
	Database string `yaml:"database"`
	Table    string `yaml:"table"`
}

// ConnectorsConfig
type ConnectorsConfig struct {
	WebHook WebHookConfig `yaml:"webhook"`
	PubSub  PubSubConfig  `yaml:"pubsub"`
	Routes  []RouteConfig `yaml:"routes"`
}

// RouteConfig
type RouteConfig struct {
	Events    []string `yaml:"events"`
	Connector string   `yaml:"connector"`
	Data      string   `yaml:"data"`
}

// WebHookConfig
type WebHookConfig struct {
	URL           string            `yaml:"url"`
	Method        string            `yaml:"method"`
	Headers       map[string]string `yaml:"headers"`
	Credentials   Credentials       `yaml:"credentials"`
	TlsSkipVerify bool              `yaml:"tls_skip_verify"`
}

// Credentials
type Credentials struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// PubSubConfig
type PubSubConfig struct {
	ProjectID string `yaml:"project_id"`
	TopicID   string `yaml:"topic_id"`
}
