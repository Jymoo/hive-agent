package capabilities

import "testing"

func TestAllContainsRequiredHolmesGPTCapabilities(t *testing.T) {
	required := []string{GetPods, GetPod, DescribePod, GetLogs, GetEvents, GetNodes, GetNamespaces, GetDeployments, GetStatefulSets, GetDaemonSets, GetServices, GetIngresses, GetResourceUsage, GetClusterHealth, GetWorkloadHealth, GetRestartAnalysis, GetFailedWorkloads, GetPendingPods, GetCrashLoopPods, GetNodeConditions}
	seen := map[string]bool{}
	for _, capability := range All() {
		if seen[capability] {
			t.Fatalf("duplicate capability %q", capability)
		}
		seen[capability] = true
	}
	for _, capability := range required {
		if !Contains(capability) {
			t.Fatalf("missing capability %q", capability)
		}
	}
}
