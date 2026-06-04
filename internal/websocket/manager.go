package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gorilla/websocket"
	"github.com/hive-sre/hive-agent/internal/auth"
	"github.com/hive-sre/hive-agent/internal/commands"
	"github.com/hive-sre/hive-agent/internal/config"
	"github.com/hive-sre/hive-agent/internal/executor"
	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/state"
	"github.com/hive-sre/hive-agent/internal/telemetry"
	"go.uber.org/zap"
)

type Manager struct {
	cfg       config.Config
	tokens    *auth.TokenStore
	reg       registration.Response
	exec      *executor.Executor
	registrar *registration.Client
	machine   *state.Machine
	metrics   *telemetry.Metrics
	log       *zap.Logger
	connected atomic.Bool
	conn      atomic.Value
}

func NewManager(cfg config.Config, tokens *auth.TokenStore, reg registration.Response, exec *executor.Executor, registrar *registration.Client, machine *state.Machine, metrics *telemetry.Metrics, log *zap.Logger) *Manager {
	return &Manager{cfg: cfg, tokens: tokens, reg: reg, exec: exec, registrar: registrar, machine: machine, metrics: metrics, log: log}
}
func (m *Manager) Connected() bool { return m.connected.Load() }
func (m *Manager) Run(ctx context.Context) {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Second
	b.MaxInterval = time.Minute
	b.MaxElapsedTime = 0
	for ctx.Err() == nil {
		if m.machine.Snapshot().State == state.Revoked {
			return
		}
		_ = m.machine.Transition(state.Connecting, "opening secure websocket tunnel")
		if err := m.connectAndServe(ctx); err != nil {
			m.log.Warn("tunnel disconnected", zap.Error(err))
			m.metrics.TunnelReconnects.Inc()
			m.connected.Store(false)
			if m.machine.Snapshot().State != state.Revoked {
				_ = m.machine.Transition(state.Offline, "websocket disconnected")
			}
		}
		d := b.NextBackOff()
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}
func (m *Manager) connectAndServe(ctx context.Context) error {
	endpoint := m.reg.TunnelURL()
	if endpoint == "" {
		endpoint = strings.Replace(m.cfg.APIEndpoint, "https://", "wss://", 1) + "/ws"
	}
	d := websocket.Dialer{TLSClientConfig: m.cfg.TLSConfig(), EnableCompression: true, HandshakeTimeout: 20 * time.Second}
	conn, _, err := d.DialContext(ctx, endpoint, http.Header{"Authorization": []string{"Bearer " + m.tokens.Get()}, "X-Hive-Cluster-ID": []string{m.reg.ClusterID}})
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(60 * time.Second)); return nil })
	m.conn.Store(conn)
	m.connected.Store(true)
	_ = m.machine.Transition(state.Online, "websocket connected")
	m.metrics.TunnelConnected.Set(1)
	defer m.metrics.TunnelConnected.Set(0)
	go m.ping(ctx, conn)
	for {
		conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		var env struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := conn.ReadJSON(&env); err != nil {
			return err
		}
		switch env.Type {
		case "command":
			var cmd commands.Request
			if err := json.Unmarshal(env.Payload, &cmd); err != nil {
				m.log.Warn("invalid command", zap.Error(err))
				continue
			}
			go m.exec.Execute(ctx, cmd, m.Send)
		case "credential_rotation":
			var rotation registration.CredentialRotation
			if err := json.Unmarshal(env.Payload, &rotation); err != nil {
				m.log.Warn("invalid credential rotation", zap.Error(err))
				continue
			}
			m.registrar.ApplyRotation(rotation)
			if rotation.WSEndpoint != "" {
				m.reg.WSEndpoint = rotation.WSEndpoint
				m.reg.WebSocketURL = rotation.WSEndpoint
			}
		case "revoke":
			_ = m.machine.Transition(state.Revoked, "agent key revoked")
			return context.Canceled
		}
	}
}
func (m *Manager) Send(ctx context.Context, msg any) error {
	c, ok := m.conn.Load().(*websocket.Conn)
	if !ok {
		return context.Canceled
	}
	c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return c.WriteJSON(msg)
}
func (m *Manager) ping(ctx context.Context, c *websocket.Conn) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
