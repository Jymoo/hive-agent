package inventory

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	hivekube "github.com/hive-sre/hive-agent/internal/kubernetes"
	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/telemetry"
)

type Runner struct {
	kube      *hivekube.Client
	api       *registration.Client
	clusterID string
	metrics   *telemetry.Metrics
	log       *zap.Logger
	queue     chan registration.InventoryEvent
}

func New(k *hivekube.Client, api *registration.Client, clusterID string, metrics *telemetry.Metrics, log *zap.Logger) *Runner {
	return &Runner{kube: k, api: api, clusterID: clusterID, metrics: metrics, log: log, queue: make(chan registration.InventoryEvent, 4096)}
}
func (r *Runner) Run(ctx context.Context, _ time.Duration) {
	factory := informers.NewSharedInformerFactory(r.kube.Clientset, 0)
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
	if !cache.WaitForCacheSync(ctx.Done(), pods.HasSynced, nodes.HasSynced, namespaces.HasSynced, deployments.HasSynced, replicasets.HasSynced, daemonsets.HasSynced, statefulsets.HasSynced, services.HasSynced, ingresses.HasSynced) {
		r.log.Warn("inventory cache sync interrupted")
	}
	r.flush(ctx)
}
func (r *Runner) bind(kind string, inf cache.SharedIndexInformer, mapper func(runtime.Object) map[string]any) {
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(obj any) { r.enqueue(kind, "upsert", obj, mapper) }, UpdateFunc: func(_, obj any) { r.enqueue(kind, "upsert", obj, mapper) }, DeleteFunc: func(obj any) { r.enqueue(kind, "delete", obj, mapper) }})
}
func (r *Runner) enqueue(kind, op string, obj any, mapper func(runtime.Object) map[string]any) {
	ro, ok := obj.(runtime.Object)
	if !ok {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			ro, _ = d.Obj.(runtime.Object)
		}
	}
	if ro == nil {
		return
	}
	meta, err := metav1.Accessor(ro)
	if err != nil {
		return
	}
	event := registration.InventoryEvent{ClusterID: r.clusterID, Type: kind, Operation: op, ResourceVersion: meta.GetResourceVersion(), Timestamp: time.Now().UTC()}
	if op != "delete" {
		event.Object = mapper(ro)
	} else {
		event.Object = map[string]any{"name": meta.GetName(), "namespace": meta.GetNamespace(), "uid": string(meta.GetUID())}
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
	batch := make([]registration.InventoryEvent, 0, 200)
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-r.queue:
			batch = append(batch, ev)
			if len(batch) >= 200 {
				r.send(ctx, &batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				r.send(ctx, &batch)
			}
		}
	}
}
func (r *Runner) send(ctx context.Context, batch *[]registration.InventoryEvent) {
	events := *batch
	*batch = make([]registration.InventoryEvent, 0, 200)
	if err := r.api.InventoryDelta(ctx, registration.InventoryDeltaPayload{ClusterID: r.clusterID, Events: events}); err != nil {
		r.metrics.InventoryFailures.Inc()
		r.log.Warn("inventory delta sync failed", zap.Error(err))
		return
	}
	r.metrics.InventorySyncs.Add(float64(len(events)))
}
func metaObj(o runtime.Object) map[string]any {
	m, _ := metav1.Accessor(o)
	return map[string]any{"name": m.GetName(), "namespace": m.GetNamespace(), "uid": string(m.GetUID()), "resourceVersion": m.GetResourceVersion(), "labels": m.GetLabels()}
}
func podObj(o runtime.Object) map[string]any {
	p := o.(*corev1.Pod)
	m := metaObj(o)
	m["phase"] = p.Status.Phase
	m["nodeName"] = p.Spec.NodeName
	return m
}
func nodeObj(o runtime.Object) map[string]any {
	n := o.(*corev1.Node)
	m := metaObj(o)
	m["version"] = n.Status.NodeInfo.KubeletVersion
	return m
}
func deploymentObj(o runtime.Object) map[string]any {
	d := o.(*appsv1.Deployment)
	m := metaObj(o)
	m["replicas"] = d.Status.Replicas
	m["availableReplicas"] = d.Status.AvailableReplicas
	return m
}
func serviceObj(o runtime.Object) map[string]any {
	s := o.(*corev1.Service)
	m := metaObj(o)
	m["type"] = s.Spec.Type
	return m
}
func ingressObj(o runtime.Object) map[string]any {
	i := o.(*networkingv1.Ingress)
	m := metaObj(o)
	m["class"] = fmt.Sprint(i.Spec.IngressClassName)
	return m
}
