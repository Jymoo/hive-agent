package config

import "testing"

func TestLoadRequiresAPIKeyPrefix(t *testing.T) {
	t.Setenv("HIVE_CLUSTER_NAME", "prod")
	t.Setenv("HIVE_API_ENDPOINT", "https://example.com")
	t.Setenv("HIVE_API_KEY", "bad")
	t.Setenv("HIVE_PROVIDER", "aws")
	_, err := Load("")
	if err == nil {
		t.Fatal("expected invalid api key")
	}
}
