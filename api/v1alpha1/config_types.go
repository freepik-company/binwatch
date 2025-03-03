package v1alpha1

// Configuration struct
type ConfigSpec struct {
	Sources struct {
		MySQL struct {
			SyncTimeoutMs    int    `yaml:"sync_timeout_ms"`
			Host             string `yaml:"host"`
			Port             uint16 `yaml:"port"`
			User             string `yaml:"user"`
			Password         string `yaml:"password"`
			Database         string `yaml:"database"`
			ServerID         uint32 `yaml:"server_id"`
			Flavor           string `yaml:"flavor"`
			ReadTimeout      int    `yaml:"read_timeout"`
			HearthbeatPeriod int    `yaml:"hearthbeat_period"`
		} `yaml:"mysql"`
	} `yaml:"sources"`
	Connectors struct {
		Webhook struct {
			Enabled bool `yaml:"enabled"`
		} `yaml:"webhook"`
	} `yaml:"connectors"`
}
