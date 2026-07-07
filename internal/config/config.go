package config

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	ClusterName   string
	APIEndpoint   string
	APIKey        string
	Provider      string
	LogLevel      string
	ListenAddress string
	TLSServerName string
	OTLPEndpoint  string
}

func Load(path string) (Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("HIVE")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()
	v.SetDefault("log_level", "info")
	v.SetDefault("listen_address", ":8080")
	if path != "" {
		if err := v.ReadInConfig(); err != nil {
			return Config{}, err
		}
	}
	cfg := Config{ClusterName: v.GetString("cluster_name"), APIEndpoint: strings.TrimRight(v.GetString("api_endpoint"), "/"), APIKey: v.GetString("api_key"), Provider: v.GetString("provider"), LogLevel: v.GetString("log_level"), ListenAddress: v.GetString("listen_address"), TLSServerName: v.GetString("tls_server_name"), OTLPEndpoint: v.GetString("otel_exporter_otlp_endpoint")}
	if cfg.ClusterName == "" || cfg.APIEndpoint == "" || cfg.APIKey == "" || cfg.Provider == "" {
		return Config{}, fmt.Errorf("clusterName, apiEndpoint, apiKey and provider are required")
	}
	if !strings.HasPrefix(cfg.APIKey, "hive_agt_") {
		return Config{}, fmt.Errorf("apiKey must use hive_agt_ prefix")
	}
	return cfg, nil
}

func (c Config) TLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, ServerName: c.TLSServerName}
}
