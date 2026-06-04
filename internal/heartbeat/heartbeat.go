package heartbeat

import (
	"context"
	"time"

	hivekube "github.com/hive-sre/hive-agent/internal/kubernetes"
	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/state"
	"github.com/hive-sre/hive-agent/internal/telemetry"
	"go.uber.org/zap"
)

type Runner struct {
	kube      *hivekube.Client
	api       *registration.Client
	clusterID string
	machine   *state.Machine
	metrics   *telemetry.Metrics
	log       *zap.Logger
}

func New(k *hivekube.Client, api *registration.Client, clusterID string, machine *state.Machine, metrics *telemetry.Metrics, log *zap.Logger) *Runner {
	return &Runner{kube: k, api: api, clusterID: clusterID, machine: machine, metrics: metrics, log: log}
}
func (r *Runner) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		r.once(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
func (r *Runner) once(ctx context.Context) {
	h, err := r.kube.Health(ctx)
	if err != nil {
		r.log.Warn("heartbeat health failed", zap.Error(err))
		r.metrics.HeartbeatFailures.Inc()
		_ = r.machine.Transition(state.Degraded, "kubernetes health collection failed")
		return
	}
	health := hivekube.ClusterHealth(h)
	snapshot := r.machine.Snapshot()
	payload := registration.HeartbeatPayload{ClusterID: r.clusterID, State: string(snapshot.State), ClusterHealth: health, Nodes: registration.NodeHealth{Ready: h.NodesReady, NotReady: h.NodesNotReady}, Pods: registration.PodHealth{Running: h.PodsRunning, Pending: h.PodsPending, Failed: h.PodsFailed}, Workloads: registration.WorkloadHealth{Deployments: h.Deployments, StatefulSets: h.StatefulSets, DaemonSets: h.DaemonSets}, Timestamp: time.Now().UTC()}
	if err := r.api.Heartbeat(ctx, payload); err != nil {
		r.log.Warn("heartbeat failed", zap.Error(err))
		r.metrics.HeartbeatFailures.Inc()
		return
	}
	r.metrics.Heartbeats.Inc()
}
