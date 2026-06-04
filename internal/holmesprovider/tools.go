package holmesprovider

const (
	ToolGetPods            = "get_pods"
	ToolGetPod             = "get_pod"
	ToolDescribePod        = "describe_pod"
	ToolGetLogs            = "get_logs"
	ToolGetEvents          = "get_events"
	ToolGetNodes           = "get_nodes"
	ToolGetNamespaces      = "get_namespaces"
	ToolGetDeployments     = "get_deployments"
	ToolGetStatefulSets    = "get_statefulsets"
	ToolGetDaemonSets      = "get_daemonsets"
	ToolGetServices        = "get_services"
	ToolGetIngresses       = "get_ingresses"
	ToolGetResourceUsage   = "get_resource_usage"
	ToolGetClusterHealth   = "get_cluster_health"
	ToolGetWorkloadHealth  = "get_workload_health"
	ToolGetRestartAnalysis = "get_restart_analysis"
	ToolGetFailedWorkloads = "get_failed_workloads"
	ToolGetPendingPods     = "get_pending_pods"
	ToolGetCrashLoopPods   = "get_crashloop_pods"
	ToolGetNodeConditions  = "get_node_conditions"
)

func KubernetesToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		tool(ToolGetPods, "List pods in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetPod, "Get one pod as structured Kubernetes data.", podParams()),
		tool(ToolDescribePod, "Describe one pod as structured Kubernetes data.", podParams()),
		tool(ToolGetLogs, "Get pod logs with optional container and tail line limits.", map[string]any{"type": "object", "properties": map[string]any{"namespace": stringParam("Pod namespace."), "name": stringParam("Pod name."), "container": stringParam("Optional container name."), "tailLines": map[string]any{"type": "integer", "description": "Maximum number of log lines to return.", "default": 200}}, "required": []string{"namespace", "name"}}),
		tool(ToolGetEvents, "List Kubernetes events in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetNodes, "List Kubernetes nodes.", emptyParams()),
		tool(ToolGetNamespaces, "List Kubernetes namespaces.", emptyParams()),
		tool(ToolGetDeployments, "List deployments in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetStatefulSets, "List StatefulSets in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetDaemonSets, "List DaemonSets in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetServices, "List services in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetIngresses, "List ingresses in a namespace or across all namespaces.", namespaceParams()),
		tool(ToolGetResourceUsage, "Summarize pod container CPU and memory requests and limits.", namespaceParams()),
		tool(ToolGetClusterHealth, "Return cluster health, node readiness, pod phases, and workload counts.", emptyParams()),
		tool(ToolGetWorkloadHealth, "Return structured workload health for deployments, StatefulSets, and DaemonSets.", namespaceParams()),
		tool(ToolGetRestartAnalysis, "Return containers with restarts and last termination state.", namespaceParams()),
		tool(ToolGetFailedWorkloads, "Return failed pods and unavailable deployments.", namespaceParams()),
		tool(ToolGetPendingPods, "Return pending pods.", namespaceParams()),
		tool(ToolGetCrashLoopPods, "Return pods with containers in CrashLoopBackOff.", namespaceParams()),
		tool(ToolGetNodeConditions, "Return node conditions for all nodes.", emptyParams()),
	}
}

func tool(name, description string, parameters ParameterSchema) ToolDefinition {
	return ToolDefinition{Name: name, Description: description, Parameters: parameters}
}

func emptyParams() ParameterSchema {
	return ParameterSchema{"type": "object", "properties": map[string]any{}, "additionalProperties": false}
}

func namespaceParams() ParameterSchema {
	return ParameterSchema{"type": "object", "properties": map[string]any{"namespace": map[string]any{"type": "string", "description": "Namespace name or * for all namespaces.", "default": "*"}}}
}

func podParams() ParameterSchema {
	return ParameterSchema{"type": "object", "properties": map[string]any{"namespace": stringParam("Pod namespace."), "name": stringParam("Pod name.")}, "required": []string{"namespace", "name"}}
}

func stringParam(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}
