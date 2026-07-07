package holmesprovider

import (
	"context"
	"testing"
	"time"
)

type fakeDispatcher struct{}

func (fakeDispatcher) DispatchTool(_ context.Context, cluster ClusterInfo, command AgentCommand) (AgentResult, error) {
	return AgentResult{ClusterID: cluster.ClusterID, ClusterName: cluster.ClusterName, Command: command.Command, RequestID: command.RequestID, Timestamp: time.Unix(100, 0).UTC(), Data: map[string]any{"cluster": cluster.ClusterName}, Status: "completed", DurationMillis: 1}, nil
}

type memoryAudit struct{ records []AuditRecord }

func (m *memoryAudit) RecordToolInvocation(_ context.Context, record AuditRecord) error {
	m.records = append(m.records, record)
	return nil
}

func TestRegistryCapabilityLookup(t *testing.T) {
	registry := NewToolRegistry(KubernetesToolDefinitions())
	registry.RegisterAgent(ClusterInfo{ClusterID: "c1", ClusterName: "prod-a", Provider: "aws", Capabilities: []string{ToolGetPods, ToolGetCrashLoopPods}})
	registry.RegisterAgent(ClusterInfo{ClusterID: "c2", ClusterName: "prod-b", Provider: "gke", Capabilities: []string{ToolGetPods}})

	clusters := registry.GetClustersForCapability(ToolGetCrashLoopPods)
	if len(clusters) != 1 || clusters[0].ClusterID != "c1" {
		t.Fatalf("expected c1 for crashloop capability, got %#v", clusters)
	}
	if len(registry.ListAvailableTools()) == 0 {
		t.Fatal("expected HolmesGPT tool definitions")
	}
}

func TestProviderAggregatesMultiClusterResults(t *testing.T) {
	registry := NewToolRegistry(KubernetesToolDefinitions())
	registry.RegisterAgent(ClusterInfo{ClusterID: "c1", ClusterName: "prod-a", Provider: "aws", Groups: []string{"production"}, Capabilities: []string{ToolGetFailedWorkloads}})
	registry.RegisterAgent(ClusterInfo{ClusterID: "c2", ClusterName: "prod-b", Provider: "gke", Groups: []string{"production"}, Capabilities: []string{ToolGetFailedWorkloads}})
	audit := &memoryAudit{}
	provider := NewProvider(registry, fakeDispatcher{}, audit)

	result, err := provider.Invoke(context.Background(), ToolInvocation{RequestID: "req-1", Tool: ToolGetFailedWorkloads, Target: Target{Group: "production"}})
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}
	if result.Context.ClusterCount != 2 || result.Context.ProviderBreakdown["aws"] != 1 || result.Context.ProviderBreakdown["gke"] != 1 {
		t.Fatalf("unexpected context: %#v", result.Context)
	}
	if len(result.Results) != 2 {
		t.Fatalf("expected two normalized results, got %d", len(result.Results))
	}
	if len(audit.records) != 1 || audit.records[0].ResultCount != 2 {
		t.Fatalf("expected audit record for two results, got %#v", audit.records)
	}
}
