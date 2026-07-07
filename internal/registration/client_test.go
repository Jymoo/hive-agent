package registration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hive-sre/hive-agent/internal/auth"
	"github.com/hive-sre/hive-agent/internal/config"
	"go.uber.org/zap"
)

func TestRegisterSendsCapabilitiesAndAcceptsWebSocketURL(t *testing.T) {
	var got Request
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/register" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(Response{ClusterID: "cluster-1", JWT: "jwt", WebSocketURL: "wss://backend/ws"})
	}))
	defer server.Close()

	client := NewClient(config.Config{APIEndpoint: server.URL}, auth.NewTokenStore(), zap.NewNop())
	client.http.SetTLSClientConfig(server.Client().Transport.(*http.Transport).TLSClientConfig)
	res, err := client.Register(context.Background(), Request{ClusterName: "prod", Provider: "aws", APIKey: "agt_test"})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if got.Capabilities == nil || len(got.Capabilities) == 0 {
		t.Fatal("expected default capabilities in registration payload")
	}
	if res.TunnelURL() != "wss://backend/ws" {
		t.Fatalf("unexpected tunnel url %q", res.TunnelURL())
	}
	if client.TokenStore().Get() != "jwt" {
		t.Fatal("expected jwt to be stored")
	}
}

func TestRegisterCapabilitiesPayload(t *testing.T) {
	var got CapabilitiesPayload
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/capabilities" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(config.Config{APIEndpoint: server.URL}, auth.NewTokenStore(), zap.NewNop())
	client.http.SetTLSClientConfig(server.Client().Transport.(*http.Transport).TLSClientConfig)
	client.TokenStore().Set("jwt")
	if err := client.RegisterCapabilities(context.Background(), "cluster-1", []string{"get_pods"}); err != nil {
		t.Fatalf("RegisterCapabilities failed: %v", err)
	}
	if got.ClusterID != "cluster-1" || len(got.Capabilities) != 1 || got.Capabilities[0] != "get_pods" {
		t.Fatalf("unexpected payload: %#v", got)
	}
}

func TestHeartbeatPayloadUsesUIStateField(t *testing.T) {
	payload := HeartbeatPayload{ClusterID: "cluster-1", State: "ONLINE", ClusterHealth: "healthy"}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) || !containsJSONField(data, "state") {
		t.Fatalf("expected state field in heartbeat json: %s", string(data))
	}
}

func containsJSONField(data []byte, field string) bool {
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return false
	}
	_, ok := obj[field]
	return ok
}
