package state

import (
	"fmt"
	"sync"
	"time"
)

type AgentState string

const (
	PendingKey AgentState = "PENDING_KEY"
	Registered AgentState = "REGISTERED"
	Connecting AgentState = "CONNECTING"
	Online     AgentState = "ONLINE"
	Degraded   AgentState = "DEGRADED"
	Offline    AgentState = "OFFLINE"
	Revoked    AgentState = "REVOKED"
)

var allowed = map[AgentState]map[AgentState]bool{
	PendingKey: {Registered: true, Revoked: true},
	Registered: {Connecting: true, Revoked: true, Offline: true},
	Connecting: {Online: true, Degraded: true, Offline: true, Revoked: true},
	Online:     {Degraded: true, Offline: true, Connecting: true, Revoked: true},
	Degraded:   {Online: true, Offline: true, Connecting: true, Revoked: true},
	Offline:    {Connecting: true, Registered: true, Revoked: true},
	Revoked:    {},
}

type Snapshot struct {
	State     AgentState `json:"state"`
	Reason    string     `json:"reason,omitempty"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

type Machine struct {
	mu       sync.RWMutex
	state    AgentState
	reason   string
	updated  time.Time
	onChange func(Snapshot)
}

func New(onChange func(Snapshot)) *Machine {
	return &Machine{state: PendingKey, updated: time.Now().UTC(), onChange: onChange}
}

func (m *Machine) Transition(next AgentState, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == next {
		m.reason = reason
		m.updated = time.Now().UTC()
		return nil
	}
	if !allowed[m.state][next] {
		return fmt.Errorf("invalid agent state transition %s -> %s", m.state, next)
	}
	m.state, m.reason, m.updated = next, reason, time.Now().UTC()
	if m.onChange != nil {
		m.onChange(Snapshot{State: m.state, Reason: m.reason, UpdatedAt: m.updated})
	}
	return nil
}

func (m *Machine) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Snapshot{State: m.state, Reason: m.reason, UpdatedAt: m.updated}
}
