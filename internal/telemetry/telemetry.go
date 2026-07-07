package telemetry

import (
	"context"
	"strings"

	"github.com/hive-sre/hive-agent/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Metrics struct {
	Heartbeats, HeartbeatFailures, InventorySyncs, InventoryFailures, Commands, CommandFailures, TunnelReconnects prometheus.Counter
	TunnelConnected                                                                                               prometheus.Gauge
	AgentState                                                                                                    *prometheus.GaugeVec
}

func NewLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	var l zapcore.Level
	if err := l.Set(strings.ToLower(level)); err == nil {
		cfg.Level.SetLevel(l)
	}
	return cfg.Build()
}
func NewMetrics() *Metrics {
	m := &Metrics{Heartbeats: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_heartbeats_total", Help: "Successful heartbeats"}), HeartbeatFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_heartbeat_failures_total", Help: "Failed heartbeats"}), InventorySyncs: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_inventory_delta_events_total", Help: "Inventory delta events sent"}), InventoryFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_inventory_failures_total", Help: "Failed inventory delta syncs"}), Commands: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_commands_total", Help: "Completed allowlisted commands"}), CommandFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_command_failures_total", Help: "Failed allowlisted commands"}), TunnelReconnects: prometheus.NewCounter(prometheus.CounterOpts{Name: "hive_agent_tunnel_reconnects_total", Help: "Tunnel reconnect attempts"}), TunnelConnected: prometheus.NewGauge(prometheus.GaugeOpts{Name: "hive_agent_tunnel_connected", Help: "Tunnel connection state"}), AgentState: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hive_agent_state", Help: "Current agent state labelled by state"}, []string{"state"})}
	prometheus.MustRegister(m.Heartbeats, m.HeartbeatFailures, m.InventorySyncs, m.InventoryFailures, m.Commands, m.CommandFailures, m.TunnelReconnects, m.TunnelConnected, m.AgentState)
	return m
}
func (m *Metrics) SetState(state string) {
	for _, s := range []string{"PENDING_KEY", "REGISTERED", "CONNECTING", "ONLINE", "DEGRADED", "OFFLINE", "REVOKED"} {
		v := 0.0
		if s == state {
			v = 1
		}
		m.AgentState.WithLabelValues(s).Set(v)
	}
}
func InitOTel(ctx context.Context, cfg config.Config) (func(context.Context) error, error) {
	if cfg.OTLPEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("hive-agent")))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
