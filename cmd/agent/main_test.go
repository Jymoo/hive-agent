package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hive-sre/hive-agent/internal/auth"
	"github.com/hive-sre/hive-agent/internal/config"
	"github.com/hive-sre/hive-agent/internal/registration"
	"go.uber.org/zap"
)

func TestProactiveTokenRefreshFiresOnInterval(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/token/refresh" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(registration.Response{ClusterID: "cluster-1", JWT: "rotated-jwt"})
	}))
	defer server.Close()

	client := registration.NewClient(config.Config{APIEndpoint: server.URL}, auth.NewTokenStore(), zap.NewNop())
	client.TokenStore().Set("initial-jwt")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proactiveTokenRefresh(ctx, client, 20*time.Millisecond, zap.NewNop())

	deadline := time.After(2 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 proactive refresh calls, got %d", calls.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if client.TokenStore().Get() != "rotated-jwt" {
		t.Fatalf("expected token store to hold the refreshed jwt, got %q", client.TokenStore().Get())
	}
}

func TestProactiveTokenRefreshStopsOnContextCancel(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(registration.Response{ClusterID: "cluster-1", JWT: "rotated-jwt"})
	}))
	defer server.Close()

	client := registration.NewClient(config.Config{APIEndpoint: server.URL}, auth.NewTokenStore(), zap.NewNop())
	client.TokenStore().Set("initial-jwt")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		proactiveTokenRefresh(ctx, client, 10*time.Millisecond, zap.NewNop())
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected proactiveTokenRefresh to return promptly after context cancel")
	}
}
