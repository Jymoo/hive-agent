package inventory

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

// newTestRunner builds a Runner with bare (unregistered) prometheus counters
// so tests never touch the global default registry — telemetry.NewMetrics()
// registers to it and panics on a second call within the same test binary.
func newTestRunner(clusterID string) *Runner {
	return &Runner{
		clusterID: clusterID,
		metrics: &telemetry.Metrics{
			InventorySyncs:    prometheus.NewCounter(prometheus.CounterOpts{Name: "test_inventory_syncs"}),
			InventoryFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_inventory_failures"}),
		},
		log:   zap.NewNop(),
		queue: make(chan registration.InventoryEvent, 10),
	}
}

func TestEnqueueStampsCurrentResyncEpoch(t *testing.T) {
	r := newTestRunner("cluster-1")
	r.resyncEpoch.Store(3)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "app-1", Namespace: "default", UID: "uid-1", ResourceVersion: "10",
	}}
	r.enqueue("pod", "upsert", pod, metaObj)

	select {
	case ev := <-r.queue:
		if ev.ResyncEpoch != 3 {
			t.Fatalf("expected resyncEpoch 3, got %d", ev.ResyncEpoch)
		}
		if ev.Type != "pod" || ev.Operation != "upsert" {
			t.Fatalf("unexpected event: %#v", ev)
		}
		if ev.Object["uid"] != "uid-1" {
			t.Fatalf("expected uid-1 in mapped object, got %#v", ev.Object)
		}
	default:
		t.Fatal("expected an event to be enqueued")
	}
}

func TestEnqueueStampsZeroEpochBeforeFirstResync(t *testing.T) {
	r := newTestRunner("cluster-1")
	// resyncEpoch left at its zero value — simulates events produced by the
	// very first full relist, before any periodic resync has occurred.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app-1", UID: "uid-1"}}
	r.enqueue("pod", "upsert", pod, metaObj)

	ev := <-r.queue
	if ev.ResyncEpoch != 0 {
		t.Fatalf("expected resyncEpoch 0, got %d", ev.ResyncEpoch)
	}
}

func TestNewBoundaryEvent(t *testing.T) {
	ev := newBoundaryEvent("cluster-1", 7)

	if ev.ClusterID != "cluster-1" {
		t.Fatalf("expected clusterID cluster-1, got %q", ev.ClusterID)
	}
	if ev.Operation != "boundary" {
		t.Fatalf("expected operation 'boundary', got %q", ev.Operation)
	}
	if ev.ResyncEpoch != 7 {
		t.Fatalf("expected resyncEpoch 7, got %d", ev.ResyncEpoch)
	}
	if ev.Object != nil {
		t.Fatalf("expected no object on a boundary marker, got %#v", ev.Object)
	}
}

func TestEnqueueDropsAndCountsFailureWhenQueueFull(t *testing.T) {
	r := newTestRunner("cluster-1")
	r.queue = make(chan registration.InventoryEvent, 1)
	r.enqueue("pod", "upsert", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", UID: "1"}}, metaObj)

	// Queue is now full; this second enqueue must be dropped, not block.
	r.enqueue("pod", "upsert", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", UID: "2"}}, metaObj)

	if len(r.queue) != 1 {
		t.Fatalf("expected queue to still hold exactly 1 event, got %d", len(r.queue))
	}
	if got := testutil.ToFloat64(r.metrics.InventoryFailures); got != 1 {
		t.Fatalf("expected InventoryFailures to be incremented once, got %v", got)
	}
}
