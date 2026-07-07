package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/hive-sre/hive-agent/internal/capabilities"
	hivekube "github.com/hive-sre/hive-agent/internal/kubernetes"
)

type Name string

const (
	GetPods                     Name = capabilities.GetPods
	GetPod                      Name = capabilities.GetPod
	DescribePod                 Name = capabilities.DescribePod
	GetLogs                     Name = capabilities.GetLogs
	GetEvents                   Name = capabilities.GetEvents
	GetNodes                    Name = capabilities.GetNodes
	GetNamespaces               Name = capabilities.GetNamespaces
	GetDeployments              Name = capabilities.GetDeployments
	GetStatefulSets             Name = capabilities.GetStatefulSets
	GetDaemonSets               Name = capabilities.GetDaemonSets
	GetServices                 Name = capabilities.GetServices
	GetIngresses                Name = capabilities.GetIngresses
	GetResourceUsage            Name = capabilities.GetResourceUsage
	GetClusterHealth            Name = capabilities.GetClusterHealth
	GetWorkloadHealth           Name = capabilities.GetWorkloadHealth
	GetRestartAnalysis          Name = capabilities.GetRestartAnalysis
	GetFailedWorkloads          Name = capabilities.GetFailedWorkloads
	GetPendingPods              Name = capabilities.GetPendingPods
	GetCrashLoopPods            Name = capabilities.GetCrashLoopPods
	GetNodeConditions           Name = capabilities.GetNodeConditions
	GetPVCs                     Name = capabilities.GetPVCs
	GetJobs                     Name = capabilities.GetJobs
	GetCronJobs                 Name = capabilities.GetCronJobs
	// Topology-critical additions
	GetReplicaSets              Name = capabilities.GetReplicaSets
	GetEndpoints                Name = capabilities.GetEndpoints
	GetNetworkPolicies          Name = capabilities.GetNetworkPolicies
	GetPersistentVolumes        Name = capabilities.GetPersistentVolumes
	GetStorageClasses           Name = capabilities.GetStorageClasses
	GetConfigMaps               Name = capabilities.GetConfigMaps
	GetSecrets                  Name = capabilities.GetSecrets
	GetServiceAccounts          Name = capabilities.GetServiceAccounts
	GetRoles                    Name = capabilities.GetRoles
	GetClusterRoles             Name = capabilities.GetClusterRoles
	GetRoleBindings             Name = capabilities.GetRoleBindings
	GetClusterRoleBindings      Name = capabilities.GetClusterRoleBindings
	GetResourceQuotas           Name = capabilities.GetResourceQuotas
	GetLimitRanges              Name = capabilities.GetLimitRanges
	GetHorizontalPodAutoscalers Name = capabilities.GetHorizontalPodAutoscalers
	GetPodDisruptionBudgets     Name = capabilities.GetPodDisruptionBudgets
	GetLeases                   Name = capabilities.GetLeases
)

var allowed = map[Name]bool{
	GetPods: true, GetPod: true, DescribePod: true, GetLogs: true, GetEvents: true, GetNodes: true,
	GetNamespaces: true, GetDeployments: true, GetStatefulSets: true, GetDaemonSets: true, GetServices: true,
	GetIngresses: true, GetResourceUsage: true, GetClusterHealth: true, GetWorkloadHealth: true,
	GetRestartAnalysis: true, GetFailedWorkloads: true, GetPendingPods: true, GetCrashLoopPods: true,
	GetNodeConditions: true, GetPVCs: true, GetJobs: true, GetCronJobs: true,
	// Topology-critical additions
	GetReplicaSets: true, GetEndpoints: true, GetNetworkPolicies: true,
	GetPersistentVolumes: true, GetStorageClasses: true, GetConfigMaps: true, GetSecrets: true,
	GetServiceAccounts: true, GetRoles: true, GetClusterRoles: true,
	GetRoleBindings: true, GetClusterRoleBindings: true,
	GetResourceQuotas: true, GetLimitRanges: true,
	GetHorizontalPodAutoscalers: true, GetPodDisruptionBudgets: true, GetLeases: true,
}

func Capabilities() []string { return capabilities.All() }

type Request struct {
	RequestID  string         `json:"requestId"`
	Command    Name           `json:"command"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type Response struct {
	Command        Name      `json:"command"`
	ClusterID      string    `json:"clusterId"`
	RequestID      string    `json:"requestId"`
	Timestamp      time.Time `json:"timestamp"`
	Data           any       `json:"data"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
	DurationMillis int64     `json:"durationMillis"`
}

type Executor struct {
	clusterID string
	kube      *hivekube.Client
}

func NewExecutor(clusterID string, kube *hivekube.Client) *Executor {
	return &Executor{clusterID: clusterID, kube: kube}
}

func (e *Executor) Execute(ctx context.Context, req Request) Response {
	start := time.Now()
	if !allowed[req.Command] {
		return e.fail(req, start, "unsupported capability")
	}
	data, err := e.run(ctx, req)
	if err != nil {
		return e.fail(req, start, err.Error())
	}
	return Response{Command: req.Command, ClusterID: e.clusterID, RequestID: req.RequestID, Timestamp: time.Now().UTC(), Data: data, Status: "success", DurationMillis: time.Since(start).Milliseconds()}
}

func (e *Executor) fail(req Request, start time.Time, msg string) Response {
	return Response{Command: req.Command, ClusterID: e.clusterID, RequestID: req.RequestID, Timestamp: time.Now().UTC(), Data: map[string]any{}, Status: "error", Error: msg, DurationMillis: time.Since(start).Milliseconds()}
}

func (e *Executor) run(ctx context.Context, req Request) (any, error) {
	ns := str(req.Parameters, "namespace", "*")
	switch req.Command {
	case GetPods:
		return e.kube.GetPods(ctx, ns)
	case GetPod, DescribePod:
		return e.kube.GetPod(ctx, str(req.Parameters, "namespace", ""), str(req.Parameters, "name", ""))
	case GetLogs:
		return e.kube.GetLogs(ctx, str(req.Parameters, "namespace", ""), str(req.Parameters, "name", ""), str(req.Parameters, "container", ""), integer(req.Parameters, "tailLines", 200))
	case GetEvents:
		return e.kube.GetEvents(ctx, ns)
	case GetNodes:
		return e.kube.GetNodes(ctx)
	case GetNamespaces:
		return e.kube.GetNamespaces(ctx)
	case GetDeployments:
		return e.kube.GetDeployments(ctx, ns)
	case GetStatefulSets:
		return e.kube.GetStatefulSets(ctx, ns)
	case GetDaemonSets:
		return e.kube.GetDaemonSets(ctx, ns)
	case GetServices:
		return e.kube.GetServices(ctx, ns)
	case GetIngresses:
		return e.kube.GetIngresses(ctx, ns)
	case GetResourceUsage:
		return e.kube.GetResourceUsage(ctx, ns)
	case GetClusterHealth:
		return e.kube.GetClusterHealth(ctx)
	case GetWorkloadHealth:
		return e.kube.GetWorkloadHealth(ctx, ns)
	case GetRestartAnalysis:
		return e.kube.GetRestartAnalysis(ctx, ns)
	case GetFailedWorkloads:
		return e.kube.GetFailedWorkloads(ctx, ns)
	case GetPendingPods:
		return e.kube.GetPendingPods(ctx, ns)
	case GetCrashLoopPods:
		return e.kube.GetCrashLoopPods(ctx, ns)
	case GetNodeConditions:
		return e.kube.GetNodeConditions(ctx)
	case GetPVCs:
		return e.kube.GetPVCs(ctx, ns)
	case GetJobs:
		return e.kube.GetJobs(ctx, ns)
	case GetCronJobs:
		return e.kube.GetCronJobs(ctx, ns)
	// Topology-critical additions
	case GetReplicaSets:
		return e.kube.GetReplicaSets(ctx, ns)
	case GetEndpoints:
		return e.kube.GetEndpoints(ctx, ns)
	case GetNetworkPolicies:
		return e.kube.GetNetworkPolicies(ctx, ns)
	case GetPersistentVolumes:
		return e.kube.GetPersistentVolumes(ctx)
	case GetStorageClasses:
		return e.kube.GetStorageClasses(ctx)
	case GetConfigMaps:
		return e.kube.GetConfigMaps(ctx, ns)
	case GetSecrets:
		return e.kube.GetSecrets(ctx, ns)
	case GetServiceAccounts:
		return e.kube.GetServiceAccounts(ctx, ns)
	case GetRoles:
		return e.kube.GetRoles(ctx, ns)
	case GetClusterRoles:
		return e.kube.GetClusterRoles(ctx)
	case GetRoleBindings:
		return e.kube.GetRoleBindings(ctx, ns)
	case GetClusterRoleBindings:
		return e.kube.GetClusterRoleBindings(ctx)
	case GetResourceQuotas:
		return e.kube.GetResourceQuotas(ctx, ns)
	case GetLimitRanges:
		return e.kube.GetLimitRanges(ctx, ns)
	case GetHorizontalPodAutoscalers:
		return e.kube.GetHorizontalPodAutoscalers(ctx, ns)
	case GetPodDisruptionBudgets:
		return e.kube.GetPodDisruptionBudgets(ctx, ns)
	case GetLeases:
		return e.kube.GetLeases(ctx, ns)
	default:
		return nil, fmt.Errorf("unsupported capability")
	}
}

func str(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return def
}

func integer(m map[string]any, key string, def int64) int64 {
	if m == nil {
		return def
	}
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		i, _ := strconv.ParseInt(string(v), 10, 64)
		return i
	default:
		return def
	}
}
