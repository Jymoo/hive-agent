package holmesprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Dispatcher interface {
	DispatchTool(ctx context.Context, cluster ClusterInfo, command AgentCommand) (AgentResult, error)
}

type AuditSink interface {
	RecordToolInvocation(ctx context.Context, record AuditRecord) error
}

type Provider struct {
	registry   *ToolRegistry
	dispatcher Dispatcher
	audit      AuditSink
}

func NewProvider(registry *ToolRegistry, dispatcher Dispatcher, audit AuditSink) *Provider {
	if registry == nil {
		registry = NewToolRegistry(KubernetesToolDefinitions())
	}
	return &Provider{registry: registry, dispatcher: dispatcher, audit: audit}
}

func (p *Provider) Registry() *ToolRegistry { return p.registry }

func (p *Provider) ListTools() []ToolDefinition { return p.registry.ListAvailableTools() }

func (p *Provider) Invoke(ctx context.Context, invocation ToolInvocation) (AggregatedResult, error) {
	started := time.Now()
	if p.dispatcher == nil {
		return AggregatedResult{}, errors.New("holmes provider dispatcher is required")
	}
	if _, ok := p.registry.Tool(invocation.Tool); !ok {
		return AggregatedResult{}, fmt.Errorf("unknown Hive HolmesGPT tool %q", invocation.Tool)
	}
	clusters, err := p.resolveClusters(invocation.Target, invocation.Tool)
	if err != nil {
		return AggregatedResult{}, err
	}
	if len(clusters) == 0 {
		return AggregatedResult{}, fmt.Errorf("no clusters available for capability %q", invocation.Tool)
	}

	results := make([]NormalizedResult, len(clusters))
	var wg sync.WaitGroup
	for i, cluster := range clusters {
		wg.Add(1)
		go func(idx int, cluster ClusterInfo) {
			defer wg.Done()
			result, err := p.dispatcher.DispatchTool(ctx, cluster, AgentCommand{RequestID: invocation.RequestID, Command: invocation.Tool, Parameters: invocation.Parameters})
			results[idx] = normalize(cluster, invocation.Tool, result, err)
		}(i, cluster)
	}
	wg.Wait()

	aggregated := AggregatedResult{RequestID: invocation.RequestID, Tool: invocation.Tool, Timestamp: time.Now().UTC(), Context: contextFor(clusters), Results: results, Data: map[string]interface{}{"results": results}}
	if p.audit != nil {
		_ = p.audit.RecordToolInvocation(ctx, AuditRecord{RequestID: invocation.RequestID, Tool: invocation.Tool, ClustersTargeted: clusterIDs(clusters), DurationMs: time.Since(started).Milliseconds(), ResultCount: len(results), Timestamp: time.Now().UTC()})
	}
	return aggregated, nil
}

func (p *Provider) resolveClusters(target Target, capability string) ([]ClusterInfo, error) {
	var selected []ClusterInfo
	if len(target.ClusterIDs) > 0 {
		for _, id := range target.ClusterIDs {
			cluster, ok := p.registry.Cluster(id)
			if !ok {
				return nil, fmt.Errorf("cluster %q is not registered", id)
			}
			selected = append(selected, cluster)
		}
	} else if target.Group != "" {
		for _, cluster := range p.registry.Clusters() {
			if has(cluster.Groups, target.Group) {
				selected = append(selected, cluster)
			}
		}
	} else if target.All {
		selected = p.registry.Clusters()
	} else {
		selected = p.registry.GetClustersForCapability(capability)
	}

	filtered := selected[:0]
	for _, cluster := range selected {
		if has(cluster.Capabilities, capability) {
			filtered = append(filtered, cluster)
		}
	}
	return filtered, nil
}

func normalize(cluster ClusterInfo, command string, result AgentResult, err error) NormalizedResult {
	if err != nil {
		return NormalizedResult{ClusterID: cluster.ClusterID, ClusterName: cluster.ClusterName, Provider: cluster.Provider, Command: command, Timestamp: time.Now().UTC(), Data: map[string]any{}, Status: "failed", Error: err.Error()}
	}
	if result.Timestamp.IsZero() {
		result.Timestamp = time.Now().UTC()
	}
	return NormalizedResult{ClusterID: cluster.ClusterID, ClusterName: cluster.ClusterName, Provider: cluster.Provider, Command: command, Timestamp: result.Timestamp, Data: result.Data, Status: result.Status, Error: result.Error}
}

func contextFor(clusters []ClusterInfo) InvestigationContext {
	ctx := InvestigationContext{ClusterCount: len(clusters), ProviderBreakdown: map[string]int{}}
	for _, cluster := range clusters {
		ctx.ProviderBreakdown[cluster.Provider]++
	}
	return ctx
}

func clusterIDs(clusters []ClusterInfo) []string {
	ids := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		ids = append(ids, cluster.ClusterID)
	}
	return ids
}
