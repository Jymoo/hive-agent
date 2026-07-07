package clustermanager

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	hivekube "github.com/hive-sre/hive-agent/internal/kubernetes"
)

// ClusterConnection holds all runtime state for a connected cluster.
// In the hive-agent (single-cluster deployment model), there is typically
// exactly one entry, but the struct is designed to support multi-cluster
// scenarios in the future.
type ClusterConnection struct {
	ClusterID     string
	Clientset     kubernetes.Interface
	DynamicClient dynamic.Interface
	RestConfig    *rest.Config
	LastHeartbeat time.Time
	Connected     bool

	mu sync.RWMutex
}

// SetConnected updates the Connected flag and, when transitioning to
// connected, also refreshes LastHeartbeat.
func (c *ClusterConnection) SetConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Connected = connected
	if connected {
		c.LastHeartbeat = time.Now().UTC()
	}
}

// IsConnected returns whether this cluster is currently active.
func (c *ClusterConnection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Connected
}

// ClusterManager is a concurrent-safe registry of ClusterConnection instances.
type ClusterManager struct {
	mu          sync.RWMutex
	connections map[string]*ClusterConnection
	log         *zap.Logger
}

// New creates an empty ClusterManager.
func New(log *zap.Logger) *ClusterManager {
	return &ClusterManager{
		connections: make(map[string]*ClusterConnection),
		log:         log,
	}
}

// Register adds or replaces the connection entry for a cluster using the
// provided hive kubernetes client and REST config.
func (m *ClusterManager) Register(clusterID string, kube *hivekube.Client, cfg *rest.Config) {
	conn := &ClusterConnection{
		ClusterID:     clusterID,
		Clientset:     kube.Clientset,
		RestConfig:    cfg,
		LastHeartbeat: time.Now().UTC(),
		Connected:     false, // transitions to true when WS is established
	}
	m.mu.Lock()
	m.connections[clusterID] = conn
	m.mu.Unlock()
	m.log.Info("cluster registered in cluster manager", zap.String("clusterID", clusterID))
}

// GetClient returns the ClusterConnection for the given cluster ID.
// The second return value is false when the cluster is not registered.
func (m *ClusterManager) GetClient(clusterID string) (*ClusterConnection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.connections[clusterID]
	return c, ok
}

// SetConnected updates the Connected flag for a registered cluster.
// It is safe to call this from any goroutine.
func (m *ClusterManager) SetConnected(clusterID string, connected bool) {
	m.mu.RLock()
	c, ok := m.connections[clusterID]
	m.mu.RUnlock()
	if !ok {
		m.log.Warn("SetConnected called for unknown cluster", zap.String("clusterID", clusterID))
		return
	}
	c.SetConnected(connected)
	state := "connected"
	if !connected {
		state = "disconnected"
	}
	m.log.Info("cluster connection state changed",
		zap.String("clusterID", clusterID),
		zap.String("state", state),
	)
}

// ClusterAwareConfig returns a *rest.Config for the given clusterID.
// This is the primary entry-point for code that needs to create additional
// Kubernetes clients scoped to a specific cluster.
func (m *ClusterManager) ClusterAwareConfig(clusterID string) (*rest.Config, error) {
	c, ok := m.GetClient(clusterID)
	if !ok {
		return nil, fmt.Errorf("cluster %q is not registered in the cluster manager", clusterID)
	}
	return c.RestConfig, nil
}

// Snapshot returns a copy of all current connection states, keyed by cluster ID.
// Useful for health/status endpoints.
func (m *ClusterManager) Snapshot() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]bool, len(m.connections))
	for id, c := range m.connections {
		out[id] = c.IsConnected()
	}
	return out
}
