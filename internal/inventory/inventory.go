package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	hivekube "github.com/hive-sre/hive-agent/internal/kubernetes"
	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/telemetry"
)

// resyncBoundaryGrace is how long to wait, after bumping resyncEpoch, before
// telling the backend the resync pass is complete. It must comfortably exceed
// the flush cadence (2s ticks / 500-item batches, see flush()) so every event
// the informer's own periodic resync produces gets batched and sent tagged
// with the new epoch before the boundary marker follows it.
const resyncBoundaryGrace = 30 * time.Second

// Queue bound: sized so a fresh full-relist burst on a large cluster
// (thousands of pods/services/etc. across 9 informers) doesn't overflow and
// silently drop events before the 2s/500-item flush can drain it.
const inventoryQueueSize = 200_000

type Runner struct {
	kube        *hivekube.Client
	api         *registration.Client
	clusterID   string
	metrics     *telemetry.Metrics
	log         *zap.Logger
	queue       chan registration.InventoryEvent
	resyncEpoch atomic.Int64
}

func New(
	k *hivekube.Client,
	api *registration.Client,
	clusterID string,
	metrics *telemetry.Metrics,
	log *zap.Logger,
) *Runner {
	return &Runner{
		kube:      k,
		api:       api,
		clusterID: clusterID,
		metrics:   metrics,
		log:       log,
		queue:     make(chan registration.InventoryEvent, inventoryQueueSize),
	}
}

func (r *Runner) Run(ctx context.Context, resyncPeriod time.Duration) {
	factory := informers.NewSharedInformerFactory(r.kube.Clientset, resyncPeriod)

	pods := factory.Core().V1().Pods().Informer()
	nodes := factory.Core().V1().Nodes().Informer()
	namespaces := factory.Core().V1().Namespaces().Informer()
	deployments := factory.Apps().V1().Deployments().Informer()
	replicasets := factory.Apps().V1().ReplicaSets().Informer()
	daemonsets := factory.Apps().V1().DaemonSets().Informer()
	statefulsets := factory.Apps().V1().StatefulSets().Informer()
	services := factory.Core().V1().Services().Informer()
	ingresses := factory.Networking().V1().Ingresses().Informer()

	r.bind("pod", pods, podObj)
	r.bind("node", nodes, nodeObj)
	r.bind("namespace", namespaces, metaObj)
	r.bind("deployment", deployments, deploymentObj)
	r.bind("replicaset", replicasets, metaObj)
	r.bind("daemonset", daemonsets, metaObj)
	r.bind("statefulset", statefulsets, metaObj)
	r.bind("service", services, serviceObj)
	r.bind("ingress", ingresses, ingressObj)

	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(
		ctx.Done(),
		pods.HasSynced,
		nodes.HasSynced,
		namespaces.HasSynced,
		deployments.HasSynced,
		replicasets.HasSynced,
		daemonsets.HasSynced,
		statefulsets.HasSynced,
		services.HasSynced,
		ingresses.HasSynced,
	) {
		r.log.Warn("inventory cache sync interrupted")
	}

	go r.runResyncCycle(ctx, resyncPeriod)
	r.flush(ctx)
}

// runResyncCycle periodically bumps resyncEpoch and, once the informer's own
// periodic resync (started with the same period) has had time to replay every
// cached object through enqueue(), emits a boundary marker event carrying the
// new epoch. The backend uses that marker to reconcile deletions that
// happened while the agent was fully down — see registration.InventoryEvent.
func (r *Runner) runResyncCycle(ctx context.Context, resyncPeriod time.Duration) {
	if resyncPeriod <= 0 {
		return
	}
	ticker := time.NewTicker(resyncPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			epoch := r.resyncEpoch.Add(1)
			select {
			case <-time.After(resyncBoundaryGrace):
			case <-ctx.Done():
				return
			}
			select {
			case r.queue <- newBoundaryEvent(r.clusterID, epoch):
			default:
				r.metrics.InventoryFailures.Inc()
				r.log.Warn("inventory delta queue full; dropped resync boundary marker", zap.Int64("epoch", epoch))
			}
		}
	}
}

// newBoundaryEvent builds the marker event that tells the backend a full
// informer resync pass (identified by epoch) has completed. It carries no
// object — the backend recognizes it purely by Operation == "boundary".
func newBoundaryEvent(clusterID string, epoch int64) registration.InventoryEvent {
	return registration.InventoryEvent{
		ClusterID:   clusterID,
		Operation:   "boundary",
		ResyncEpoch: epoch,
		Timestamp:   time.Now().UTC(),
	}
}

func (r *Runner) bind(
	kind string,
	inf cache.SharedIndexInformer,
	mapper func(runtime.Object) map[string]any,
) {
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			r.enqueue(kind, "upsert", obj, mapper)
		},
		UpdateFunc: func(_, obj any) {
			r.enqueue(kind, "upsert", obj, mapper)
		},
		DeleteFunc: func(obj any) {
			r.enqueue(kind, "delete", obj, mapper)
		},
	})
}

func (r *Runner) enqueue(
	kind,
	op string,
	obj any,
	mapper func(runtime.Object) map[string]any,
) {
	ro, ok := obj.(runtime.Object)
	if !ok {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			ro, _ = d.Obj.(runtime.Object)
		}
	}

	if ro == nil {
		return
	}

	objMeta, err := meta.Accessor(ro)
	if err != nil {
		return
	}

	event := registration.InventoryEvent{
		ClusterID:      r.clusterID,
		Type:           kind,
		Operation:      op,
		ResourceVersion: objMeta.GetResourceVersion(),
		Timestamp:      time.Now().UTC(),
		ResyncEpoch:    r.resyncEpoch.Load(),
	}

	if op != "delete" {
		event.Object = mapper(ro)
	} else {
		event.Object = map[string]any{
			"name":      objMeta.GetName(),
			"namespace": objMeta.GetNamespace(),
			"uid":       string(objMeta.GetUID()),
		}
	}

	select {
	case r.queue <- event:
	default:
		r.metrics.InventoryFailures.Inc()
		r.log.Warn("inventory delta queue full")
	}
}

func (r *Runner) flush(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	batch := make([]registration.InventoryEvent, 0, 500)

	for {
		select {
		case <-ctx.Done():
			return

		case ev := <-r.queue:
			batch = append(batch, ev)

			if len(batch) >= 500 {
				go r.sendEvents(ctx, batch)
				batch = make([]registration.InventoryEvent, 0, 500)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				go r.sendEvents(ctx, batch)
				batch = make([]registration.InventoryEvent, 0, 500)
			}
		}
	}
}

func (r *Runner) sendEvents(
	ctx context.Context,
	events []registration.InventoryEvent,
) {
	if err := r.api.InventoryDelta(
		ctx,
		registration.InventoryDeltaPayload{
			ClusterID: r.clusterID,
			Events:    events,
		},
	); err != nil {
		r.metrics.InventoryFailures.Inc()
		if errors.Is(err, registration.ErrUnauthorized) {
			r.log.Warn("inventory delta sync JWT rejected (401/403); attempting re-registration", zap.Error(err))
			if reErr := r.api.Reregister(ctx); reErr != nil {
				r.log.Error("inventory re-registration failed", zap.Error(reErr))
			}
		} else {
			r.log.Warn("inventory delta sync failed", zap.Error(err))
		}
		return
	}

	r.metrics.InventorySyncs.Add(float64(len(events)))
}

func metaObj(o runtime.Object) map[string]any {
	m, err := meta.Accessor(o)
	if err != nil {
		return map[string]any{}
	}
	return map[string]any{
		"name":            m.GetName(),
		"namespace":       m.GetNamespace(),
		"uid":             string(m.GetUID()),
		"resourceVersion": m.GetResourceVersion(),
		"labels":          m.GetLabels(),
	}
}

func podObj(o runtime.Object) map[string]any {
	p := o.(*corev1.Pod)
	m := metaObj(o)
	m["phase"] = p.Status.Phase
	m["nodeName"] = p.Spec.NodeName
	m["hostIP"] = p.Status.HostIP
	m["podIP"] = p.Status.PodIP
	// ownerReferences for Pod→ReplicaSet→Deployment chain
	ownerRefs := make([]map[string]any, 0, len(p.OwnerReferences))
	for _, ref := range p.OwnerReferences {
		ownerRefs = append(ownerRefs, map[string]any{
			"apiVersion": ref.APIVersion,
			"kind":       ref.Kind,
			"name":       ref.Name,
			"uid":        string(ref.UID),
		})
	}
	m["ownerReferences"] = ownerRefs
	return m
}

func nodeObj(o runtime.Object) map[string]any {
	n := o.(*corev1.Node)
	m := metaObj(o)
	m["version"] = n.Status.NodeInfo.KubeletVersion
	conditions := make([]map[string]any, 0, len(n.Status.Conditions))
	for _, c := range n.Status.Conditions {
		conditions = append(conditions, map[string]any{
			"type":   string(c.Type),
			"status": string(c.Status),
		})
	}
	m["conditions"] = conditions
	return m
}

func deploymentObj(o runtime.Object) map[string]any {
	d := o.(*appsv1.Deployment)
	m := metaObj(o)
	m["replicas"] = d.Status.Replicas
	m["availableReplicas"] = d.Status.AvailableReplicas
	// selector.matchLabels for Service→Deployment matching
	if d.Spec.Selector != nil {
		m["selector"] = d.Spec.Selector.MatchLabels
	}
	m["templateLabels"] = d.Spec.Template.Labels
	return m
}

func serviceObj(o runtime.Object) map[string]any {
	s := o.(*corev1.Service)
	m := metaObj(o)
	m["type"] = s.Spec.Type
	m["clusterIP"] = s.Spec.ClusterIP
	// spec.selector — primary key for Service→Pod/Deployment matching
	m["selector"] = s.Spec.Selector
	ports := make([]map[string]any, 0, len(s.Spec.Ports))
	for _, p := range s.Spec.Ports {
		ports = append(ports, map[string]any{
			"name":       p.Name,
			"port":       p.Port,
			"targetPort": p.TargetPort.String(),
			"protocol":   string(p.Protocol),
		})
	}
	m["ports"] = ports
	return m
}

func ingressObj(o runtime.Object) map[string]any {
	i := o.(*networkingv1.Ingress)
	m := metaObj(o)
	m["class"] = fmt.Sprint(i.Spec.IngressClassName)
	// spec.rules for Ingress→Service edges
	rules := make([]map[string]any, 0, len(i.Spec.Rules))
	for _, r := range i.Spec.Rules {
		paths := []map[string]any{}
		if r.HTTP != nil {
			for _, p := range r.HTTP.Paths {
				svcName := ""
				if p.Backend.Service != nil {
					svcName = p.Backend.Service.Name
				}
				paths = append(paths, map[string]any{"path": p.Path, "serviceName": svcName})
			}
		}
		rules = append(rules, map[string]any{"host": r.Host, "paths": paths})
	}
	m["rules"] = rules
	return m
}
