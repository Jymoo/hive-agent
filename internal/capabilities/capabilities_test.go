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

func TestAllContainsPeriodicResync(t *testing.T) {
	// The backend gates its stale-resource reconciliation sweep on this
	// capability token — an agent that never advertises it must never have
	// resources swept, so this must always be present in All().
	if !Contains(PeriodicResync) {
		t.Fatal("expected PeriodicResync capability in All()")
	}
}
