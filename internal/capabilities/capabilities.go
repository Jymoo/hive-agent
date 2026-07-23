package capabilities

const (
	GetPods                    = "get_pods"
	GetPod                     = "get_pod"
	DescribePod                = "describe_pod"
	GetLogs                    = "get_logs"
	GetEvents                  = "get_events"
	GetNodes                   = "get_nodes"
	GetNamespaces              = "get_namespaces"
	GetDeployments             = "get_deployments"
	GetStatefulSets            = "get_statefulsets"
	GetDaemonSets              = "get_daemonsets"
	GetServices                = "get_services"
	GetIngresses               = "get_ingresses"
	GetResourceUsage           = "get_resource_usage"
	GetClusterHealth           = "get_cluster_health"
	GetWorkloadHealth          = "get_workload_health"
	GetRestartAnalysis         = "get_restart_analysis"
	GetFailedWorkloads         = "get_failed_workloads"
	GetPendingPods             = "get_pending_pods"
	GetCrashLoopPods           = "get_crashloop_pods"
	GetNodeConditions          = "get_node_conditions"
	GetPVCs                    = "get_pvcs"
	GetJobs                    = "get_jobs"
	GetCronJobs                = "get_cronjobs"
	// Topology-critical additions
	GetReplicaSets             = "get_replicasets"
	GetEndpoints               = "get_endpoints"
	GetNetworkPolicies         = "get_networkpolicies"
	GetPersistentVolumes       = "get_persistentvolumes"
	GetStorageClasses          = "get_storageclasses"
	GetConfigMaps              = "get_configmaps"
	GetSecrets                 = "get_secrets"
	GetServiceAccounts         = "get_serviceaccounts"
	GetRoles                   = "get_roles"
	GetClusterRoles            = "get_clusterroles"
	GetRoleBindings            = "get_rolebindings"
	GetClusterRoleBindings     = "get_clusterrolebindings"
	GetResourceQuotas          = "get_resourcequotas"
	GetLimitRanges             = "get_limitranges"
	GetHorizontalPodAutoscalers = "get_horizontalpodautoscalers"
	GetPodDisruptionBudgets    = "get_poddisruptionbudgets"
	GetLeases                  = "get_leases"
	// PeriodicResync is not a read-verb command capability like the others —
	// it tells the backend this agent performs a periodic full informer
	// resync and emits resync-boundary markers on the inventory delta stream,
	// so the backend's stale-resource reconciliation sweep is safe to run for
	// this cluster. Older agents that never send boundary markers must never
	// have their resources swept, hence the explicit gate.
	PeriodicResync             = "periodic_resync"
)

func All() []string {
	return []string{
		GetPods, GetPod, DescribePod, GetLogs, GetEvents,
		GetNodes, GetNamespaces, GetDeployments, GetStatefulSets, GetDaemonSets,
		GetServices, GetIngresses, GetResourceUsage, GetClusterHealth, GetWorkloadHealth,
		GetRestartAnalysis, GetFailedWorkloads, GetPendingPods, GetCrashLoopPods,
		GetNodeConditions, GetPVCs, GetJobs, GetCronJobs,
		// Topology-critical additions
		GetReplicaSets, GetEndpoints, GetNetworkPolicies,
		GetPersistentVolumes, GetStorageClasses, GetConfigMaps, GetSecrets,
		GetServiceAccounts, GetRoles, GetClusterRoles, GetRoleBindings, GetClusterRoleBindings,
		GetResourceQuotas, GetLimitRanges, GetHorizontalPodAutoscalers,
		GetPodDisruptionBudgets, GetLeases,
		PeriodicResync,
	}
}

func Contains(capability string) bool {
	for _, item := range All() {
		if item == capability {
			return true
		}
	}
	return false
}
