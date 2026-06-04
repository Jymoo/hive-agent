package executor

import (
	"context"
	"time"

	"github.com/hive-sre/hive-agent/internal/commands"
	"github.com/hive-sre/hive-agent/internal/telemetry"
	"go.uber.org/zap"
)

type Sender func(context.Context, any) error

type Executor struct {
	clusterID string
	commands  *commands.Executor
	log       *zap.Logger
	metrics   *telemetry.Metrics
}

func New(clusterID string, c *commands.Executor, log *zap.Logger, metrics *telemetry.Metrics) *Executor {
	return &Executor{clusterID: clusterID, commands: c, log: log, metrics: metrics}
}
func (e *Executor) Execute(ctx context.Context, cmd commands.Request, send Sender) {
	_ = send(ctx, map[string]any{"type": "command_status", "payload": map[string]any{"requestId": cmd.RequestID, "clusterId": e.clusterID, "phase": "running", "timestamp": time.Now().UTC()}})
	res := e.commands.Execute(ctx, cmd)
	if res.Status == "failed" {
		e.metrics.CommandFailures.Inc()
	} else {
		e.metrics.Commands.Inc()
	}
	_ = send(ctx, map[string]any{"type": "command_result", "payload": res})
}
