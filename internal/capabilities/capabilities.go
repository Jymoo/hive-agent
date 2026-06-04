package capabilities

const (
	GetPods            = "get_pods"
	GetPod             = "get_pod"
	DescribePod        = "describe_pod"
	GetLogs            = "get_logs"
	GetEvents          = "get_events"
	GetNodes           = "get_nodes"
	GetNamespaces      = "get_namespaces"
	GetDeployments     = "get_deployments"
	GetStatefulSets    = "get_statefulsets"
	GetDaemonSets      = "get_daemonsets"
	GetServices        = "get_services"
	GetIngresses       = "get_ingresses"
	GetResourceUsage   = "get_resource_usage"
	GetClusterHealth   = "get_cluster_health"
	GetWorkloadHealth  = "get_workload_health"
	GetRestartAnalysis = "get_restart_analysis"
	GetFailedWorkloads = "get_failed_workloads"
	GetPendingPods     = "get_pending_pods"
	GetCrashLoopPods   = "get_crashloop_pods"
	GetNodeConditions  = "get_node_conditions"
)

func All() []string {
	return []string{GetPods, GetPod, DescribePod, GetLogs, GetEvents, GetNodes, GetNamespaces, GetDeployments, GetStatefulSets, GetDaemonSets, GetServices, GetIngresses, GetResourceUsage, GetClusterHealth, GetWorkloadHealth, GetRestartAnalysis, GetFailedWorkloads, GetPendingPods, GetCrashLoopPods, GetNodeConditions}
}

func Contains(capability string) bool {
	for _, item := range All() {
		if item == capability {
			return true
		}
	}
	return false
}
