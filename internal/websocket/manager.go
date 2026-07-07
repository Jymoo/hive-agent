package websocket

import (
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gorilla/websocket"
	"github.com/hive-sre/hive-agent/internal/auth"
	"github.com/hive-sre/hive-agent/internal/clustermanager"
	"github.com/hive-sre/hive-agent/internal/commands"
	"github.com/hive-sre/hive-agent/internal/config"
	"github.com/hive-sre/hive-agent/internal/executor"
	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/state"
	"github.com/hive-sre/hive-agent/internal/telemetry"
	"go.uber.org/zap"
)

type Manager struct {
	cfg            config.Config
	tokens         *auth.TokenStore
	reg            registration.Response
	exec           *executor.Executor
	registrar      *registration.Client
	machine        *state.Machine
	metrics        *telemetry.Metrics
	log            *zap.Logger
	clusterManager *clustermanager.ClusterManager
	connected      atomic.Bool
	conn           atomic.Value
	writeMu        sync.Mutex
}

func NewManager(cfg config.Config, tokens *auth.TokenStore, reg registration.Response, exec *executor.Executor, registrar *registration.Client, machine *state.Machine, metrics *telemetry.Metrics, log *zap.Logger, cm *clustermanager.ClusterManager) *Manager {
	return &Manager{cfg: cfg, tokens: tokens, reg: reg, exec: exec, registrar: registrar, machine: machine, metrics: metrics, log: log, clusterManager: cm}
}
func (m *Manager) Connected() bool { return m.connected.Load() }

// Run is the top-level reconnect loop. It is intentionally infinite (MaxElapsedTime=0)
// and reconnects as fast as possible when the tunnel drops while previously connected.
func (m *Manager) Run(ctx context.Context) {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 500 * time.Millisecond // fast first retry when never-connected
	b.MaxInterval = 30 * time.Second           // cap at 30s (not 60s) for faster recovery
	b.MaxElapsedTime = 0                        // retry forever

	for ctx.Err() == nil {
		if m.machine.Snapshot().State == state.Revoked {
			return
		}
		_ = m.machine.Transition(state.Connecting, "opening secure websocket tunnel")

		wasConnected := m.Connected()
		err := m.connectAndServe(ctx)

		if err != nil {
			m.log.Warn("tunnel disconnected", zap.Error(err))
			m.metrics.TunnelReconnects.Inc()
			m.connected.Store(false)
			if m.clusterManager != nil {
				m.clusterManager.SetConnected(m.reg.ClusterID, false)
			}
			if m.machine.Snapshot().State != state.Revoked {
				_ = m.machine.Transition(state.Offline, "websocket disconnected")
			}

			if wasConnected {
				// Was live — reset backoff and reconnect immediately without any sleep.
				// The connection was healthy; this is a transient network blip.
				b.Reset()
				m.log.Info("tunnel was active; reconnecting immediately")
				continue
			}
		}

		// Never-connected or initial dial failed — use exponential back-off.
		d := jitter(b.NextBackOff())
		m.log.Info("reconnecting after backoff", zap.Duration("delay", d))
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

// jitter applies ±20% random jitter to a duration so that multiple
// agents that restart simultaneously do not all reconnect at the same time.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := float64(d) * 0.20
	offset := (rand.Float64()*2 - 1) * delta
	return time.Duration(float64(d) + offset)
}

func (m *Manager) connectAndServe(ctx context.Context) error {
	endpoint := m.reg.TunnelURL()
	if endpoint == "" {
		// Build WS endpoint from API endpoint, handling both http and https.
		ep := m.cfg.APIEndpoint
		ep = strings.Replace(ep, "https://", "wss://", 1)
		ep = strings.Replace(ep, "http://", "ws://", 1)
		endpoint = ep + "/ws"
	}

	// Use a net.Dialer with OS-level TCP keepalives so that NAT tables never
	// expire the underlying TCP connection, even during long idle periods.
	netDialer := &net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 15 * time.Second, // send TCP keepalive every 15s
	}
	d := websocket.Dialer{
		NetDial:           netDialer.Dial,
		TLSClientConfig:   m.cfg.TLSConfig(),
		EnableCompression: true,
		HandshakeTimeout:  20 * time.Second,
	}

	conn, resp, err := d.DialContext(ctx, endpoint, http.Header{
		"Authorization":   []string{"Bearer " + m.tokens.Get()},
		"X-Hive-Cluster-ID": []string{m.reg.ClusterID},
	})
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			m.log.Warn("websocket dial unauthorized (401/403); triggering re-registration", zap.Error(err))
			if reErr := m.registrar.Reregister(ctx); reErr != nil {
				m.log.Error("websocket re-registration failed", zap.Error(reErr))
			}
		}
		return err
	}
	defer conn.Close()

	// PongHandler resets read deadline whenever a TCP-level pong arrives.
	// We get pongs every 15s (our ping interval), so the deadline is never hit
	// as long as the network path is alive.
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	m.conn.Store(conn)
	m.connected.Store(true)
	_ = m.machine.Transition(state.Online, "websocket connected")
	if m.clusterManager != nil {
		m.clusterManager.SetConnected(m.reg.ClusterID, true)
	}
	m.metrics.TunnelConnected.Set(1)
	defer m.metrics.TunnelConnected.Set(0)

	// Send TCP-level WebSocket pings every 15s to:
	//   1. Reset the backend's application-level keepalive timer.
	//   2. Keep NAT tables alive (in addition to OS-level TCP keepalive).
	//   3. Detect half-open connections within one ping interval.
	go m.ping(ctx, conn)

	for {
		// Read deadline: 3× ping interval (45s). A pong resets it, so the
		// deadline is only triggered if three consecutive pings go unanswered.
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		var env struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := conn.ReadJSON(&env); err != nil {
			return err
		}
		// Reset the deadline on every message — any traffic keeps the tunnel alive.
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))

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
		case "ping":
			// Application-level ping from the server — reply immediately so the
			// server-side ping_loop records the receipt and keeps the connection alive.
			go func() {
				_ = m.Send(ctx, map[string]any{
					"type":    "pong",
					"payload": map[string]any{"ts": float64(time.Now().UnixMilli()) / 1000.0},
				})
			}()
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
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return c.WriteJSON(msg)
}

// ping sends a WebSocket-level PingMessage every 15 seconds.
// The gorilla/websocket library delivers these as TCP-level WebSocket frames;
// the peer's PongHandler resets the read deadline.
func (m *Manager) ping(ctx context.Context, c *websocket.Conn) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.writeMu.Lock()
			c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := c.WriteMessage(websocket.PingMessage, nil)
			m.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}
