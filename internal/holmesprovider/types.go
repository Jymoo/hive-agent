package holmesprovider

import "time"

type ParameterSchema map[string]any

type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  ParameterSchema `json:"parameters"`
}

type ClusterInfo struct {
	ClusterID    string    `json:"clusterId"`
	ClusterName  string    `json:"clusterName"`
	Provider     string    `json:"provider"`
	Groups       []string  `json:"groups,omitempty"`
	Capabilities []string  `json:"capabilities"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type Target struct {
	ClusterIDs []string `json:"clusterIds,omitempty"`
	Group      string   `json:"group,omitempty"`
	All        bool     `json:"all,omitempty"`
}

type ToolInvocation struct {
	RequestID  string         `json:"requestId"`
	Tool       string         `json:"tool"`
	Target     Target         `json:"target"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type AgentCommand struct {
	RequestID  string         `json:"requestId"`
	Command    string         `json:"command"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type AgentResult struct {
	ClusterID      string    `json:"clusterId"`
	ClusterName    string    `json:"clusterName,omitempty"`
	Command        string    `json:"command"`
	RequestID      string    `json:"requestId"`
	Timestamp      time.Time `json:"timestamp"`
	Data           any       `json:"data"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
	DurationMillis int64     `json:"durationMillis"`
}

type NormalizedResult struct {
	ClusterID   string    `json:"clusterId"`
	ClusterName string    `json:"clusterName"`
	Provider    string    `json:"provider"`
	Command     string    `json:"command"`
	Timestamp   time.Time `json:"timestamp"`
	Data        any       `json:"data"`
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
}

type InvestigationContext struct {
	ClusterCount      int            `json:"clusterCount"`
	ProviderBreakdown map[string]int `json:"providerBreakdown"`
}

type AggregatedResult struct {
	RequestID string                 `json:"requestId"`
	Tool      string                 `json:"tool"`
	Timestamp time.Time              `json:"timestamp"`
	Context   InvestigationContext   `json:"context"`
	Results   []NormalizedResult     `json:"results"`
	Data      map[string]interface{} `json:"data"`
}

type AuditRecord struct {
	RequestID        string    `json:"requestId"`
	Tool             string    `json:"tool"`
	ClustersTargeted []string  `json:"clustersTargeted"`
	DurationMs       int64     `json:"durationMs"`
	ResultCount      int       `json:"resultCount"`
	Timestamp        time.Time `json:"timestamp"`
}
