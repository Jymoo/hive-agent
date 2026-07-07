package holmesprovider

import (
	"sort"
	"sync"
	"time"
)

type ToolRegistry struct {
	mu       sync.RWMutex
	clusters map[string]ClusterInfo
	tools    map[string]ToolDefinition
}

func NewToolRegistry(definitions []ToolDefinition) *ToolRegistry {
	r := &ToolRegistry{clusters: map[string]ClusterInfo{}, tools: map[string]ToolDefinition{}}
	for _, def := range definitions {
		r.tools[def.Name] = def
	}
	return r
}

func (r *ToolRegistry) RegisterTool(def ToolDefinition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[def.Name] = def
}

func (r *ToolRegistry) RegisterAgent(info ClusterInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if info.UpdatedAt.IsZero() {
		info.UpdatedAt = time.Now().UTC()
	}
	r.clusters[info.ClusterID] = info
}

func (r *ToolRegistry) UnregisterAgent(clusterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clusters, clusterID)
}

func (r *ToolRegistry) GetCapabilities(clusterID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.clusters[clusterID]
	if !ok {
		return nil
	}
	return append([]string(nil), info.Capabilities...)
}

func (r *ToolRegistry) GetClustersForCapability(capability string) []ClusterInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []ClusterInfo{}
	for _, info := range r.clusters {
		if has(info.Capabilities, capability) {
			out = append(out, copyCluster(info))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })
	return out
}

func (r *ToolRegistry) ListAvailableTools() []ToolDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ToolDefinition, 0, len(r.tools))
	for _, def := range r.tools {
		out = append(out, def)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *ToolRegistry) Tool(name string) (ToolDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.tools[name]
	return def, ok
}

func (r *ToolRegistry) Cluster(clusterID string) (ClusterInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.clusters[clusterID]
	if !ok {
		return ClusterInfo{}, false
	}
	return copyCluster(info), true
}

func (r *ToolRegistry) Clusters() []ClusterInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ClusterInfo, 0, len(r.clusters))
	for _, info := range r.clusters {
		out = append(out, copyCluster(info))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })
	return out
}

func copyCluster(info ClusterInfo) ClusterInfo {
	info.Groups = append([]string(nil), info.Groups...)
	info.Capabilities = append([]string(nil), info.Capabilities...)
	return info
}

func has(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
