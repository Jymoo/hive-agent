package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Client struct {
	Clientset kubernetes.Interface
	log       *zap.Logger
}
type Metadata struct {
	KubernetesVersion, ClusterUID string
	NodeCount                     int
}
type HealthSnapshot struct{ NodesReady, NodesNotReady, PodsRunning, PodsPending, PodsFailed, Deployments, StatefulSets, DaemonSets int }

func NewInCluster(_ context.Context, log *zap.Logger) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{Clientset: cs, log: log}, nil
}
func (c *Client) ClusterMetadata(ctx context.Context) (Metadata, error) {
	ver, err := c.Clientset.Discovery().ServerVersion()
	if err != nil {
		return Metadata{}, err
	}
	ns, err := c.Clientset.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return Metadata{}, err
	}
	nodes, err := c.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return Metadata{}, err
	}
	uid := string(ns.UID)
	if uid == "" {
		sum := sha256.Sum256([]byte(ver.GitVersion))
		uid = hex.EncodeToString(sum[:8])
	}
	return Metadata{KubernetesVersion: ver.GitVersion, ClusterUID: uid, NodeCount: len(nodes.Items)}, nil
}
func (c *Client) Health(ctx context.Context) (HealthSnapshot, error) {
	var out HealthSnapshot
	nodes, err := c.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	for _, n := range nodes.Items {
		if nodeReady(n) {
			out.NodesReady++
		} else {
			out.NodesNotReady++
		}
	}
	pods, err := c.Clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	for _, p := range pods.Items {
		switch p.Status.Phase {
		case corev1.PodRunning:
			out.PodsRunning++
		case corev1.PodPending:
			out.PodsPending++
		case corev1.PodFailed:
			out.PodsFailed++
		}
	}
	deps, err := c.Clientset.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	out.Deployments = len(deps.Items)
	ss, err := c.Clientset.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	out.StatefulSets = len(ss.Items)
	ds, err := c.Clientset.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	out.DaemonSets = len(ds.Items)
	return out, nil
}
func nodeReady(n corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
func ClusterHealth(h HealthSnapshot) string {
	if h.NodesNotReady > 0 || h.PodsFailed > 0 {
		return "degraded"
	}
	return "healthy"
}

func (c *Client) GetPods(ctx context.Context, namespace string) ([]map[string]any, error) {
	pods, err := c.Clientset.CoreV1().Pods(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(pods.Items))
	for _, p := range pods.Items {
		out = append(out, podMap(p))
	}
	return out, nil
}
func (c *Client) GetFailedPods(ctx context.Context, namespace string) ([]map[string]any, error) {
	pods, err := c.Clientset.CoreV1().Pods(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, p := range pods.Items {
		if podFailing(p) {
			out = append(out, podMap(p))
		}
	}
	return out, nil
}
func (c *Client) GetPod(ctx context.Context, namespace, name string) (map[string]any, error) {
	if namespace == "" || name == "" {
		return nil, fmt.Errorf("namespace and name are required")
	}
	p, err := c.Clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return podMap(*p), nil
}
func (c *Client) GetLogs(ctx context.Context, namespace, name, container string, tail int64) (map[string]any, error) {
	if namespace == "" || name == "" {
		return nil, fmt.Errorf("namespace and name are required")
	}
	opts := &corev1.PodLogOptions{Container: container, TailLines: &tail}
	b, err := c.Clientset.CoreV1().Pods(namespace).GetLogs(name, opts).DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"namespace": namespace, "name": name, "container": container, "tailLines": tail, "logs": string(b)}, nil
}
func (c *Client) GetEvents(ctx context.Context, namespace string) ([]map[string]any, error) {
	ev, err := c.Clientset.CoreV1().Events(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(ev.Items))
	for _, e := range ev.Items {
		out = append(out, map[string]any{"name": e.Name, "namespace": e.Namespace, "reason": e.Reason, "message": e.Message, "type": e.Type, "involvedObject": e.InvolvedObject.Name, "resourceVersion": e.ResourceVersion})
	}
	return out, nil
}
func (c *Client) GetNodes(ctx context.Context) ([]map[string]any, error) {
	nodes, err := c.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		out = append(out, nodeMap(n))
	}
	return out, nil
}
func (c *Client) GetDeployments(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.AppsV1().Deployments(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, d := range items.Items {
		out = append(out, deploymentMap(d))
	}
	return out, nil
}
func (c *Client) GetServices(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().Services(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, s := range items.Items {
		out = append(out, serviceMap(s))
	}
	return out, nil
}
func (c *Client) GetIngresses(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.NetworkingV1().Ingresses(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, i := range items.Items {
		out = append(out, ingressMap(i))
	}
	return out, nil
}

func (c *Client) GetNamespaces(ctx context.Context) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, n := range items.Items {
		m := base(n.Name, "", string(n.UID), n.ResourceVersion, n.Labels)
		m["phase"] = n.Status.Phase
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetStatefulSets(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.AppsV1().StatefulSets(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, s := range items.Items {
		m := base(s.Name, s.Namespace, string(s.UID), s.ResourceVersion, s.Labels)
		m["replicas"] = s.Status.Replicas
		m["readyReplicas"] = s.Status.ReadyReplicas
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetDaemonSets(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.AppsV1().DaemonSets(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, d := range items.Items {
		m := base(d.Name, d.Namespace, string(d.UID), d.ResourceVersion, d.Labels)
		m["desiredNumberScheduled"] = d.Status.DesiredNumberScheduled
		m["numberReady"] = d.Status.NumberReady
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetResourceUsage(ctx context.Context, namespace string) (map[string]any, error) {
	pods, err := c.Clientset.CoreV1().Pods(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var cpuRequests, cpuLimits, memoryRequests, memoryLimits int64
	for _, p := range pods.Items {
		for _, ctr := range p.Spec.Containers {
			if q, ok := ctr.Resources.Requests[corev1.ResourceCPU]; ok {
				cpuRequests += q.MilliValue()
			}
			if q, ok := ctr.Resources.Limits[corev1.ResourceCPU]; ok {
				cpuLimits += q.MilliValue()
			}
			if q, ok := ctr.Resources.Requests[corev1.ResourceMemory]; ok {
				memoryRequests += q.Value()
			}
			if q, ok := ctr.Resources.Limits[corev1.ResourceMemory]; ok {
				memoryLimits += q.Value()
			}
		}
	}
	return map[string]any{"namespace": namespace, "podCount": len(pods.Items), "cpuRequestsMilli": cpuRequests, "cpuLimitsMilli": cpuLimits, "memoryRequestsBytes": memoryRequests, "memoryLimitsBytes": memoryLimits}, nil
}

func (c *Client) GetClusterHealth(ctx context.Context) (map[string]any, error) {
	h, err := c.Health(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"clusterHealth": ClusterHealth(h), "nodes": map[string]int{"ready": h.NodesReady, "notReady": h.NodesNotReady}, "pods": map[string]int{"running": h.PodsRunning, "pending": h.PodsPending, "failed": h.PodsFailed}, "workloads": map[string]int{"deployments": h.Deployments, "statefulsets": h.StatefulSets, "daemonsets": h.DaemonSets}}, nil
}

func (c *Client) GetWorkloadHealth(ctx context.Context, namespace string) (map[string]any, error) {
	deps, err := c.GetDeployments(ctx, namespace)
	if err != nil {
		return nil, err
	}
	sts, err := c.GetStatefulSets(ctx, namespace)
	if err != nil {
		return nil, err
	}
	ds, err := c.GetDaemonSets(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return map[string]any{"namespace": namespace, "deployments": deps, "statefulsets": sts, "daemonsets": ds}, nil
}

func (c *Client) GetRestartAnalysis(ctx context.Context, namespace string) ([]map[string]any, error) {
	pods, err := c.Clientset.CoreV1().Pods(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.RestartCount > 0 {
				out = append(out, map[string]any{"namespace": p.Namespace, "pod": p.Name, "container": cs.Name, "restartCount": cs.RestartCount, "lastTerminationState": cs.LastTerminationState})
			}
		}
	}
	return out, nil
}

func (c *Client) GetFailedWorkloads(ctx context.Context, namespace string) (map[string]any, error) {
	pods, err := c.GetFailedPods(ctx, namespace)
	if err != nil {
		return nil, err
	}
	deployments, err := c.Clientset.AppsV1().Deployments(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	failedDeployments := []map[string]any{}
	for _, d := range deployments.Items {
		if d.Status.UnavailableReplicas > 0 {
			m := deploymentMap(d)
			m["unavailableReplicas"] = d.Status.UnavailableReplicas
			failedDeployments = append(failedDeployments, m)
		}
	}
	return map[string]any{"namespace": namespace, "pods": pods, "deployments": failedDeployments}, nil
}

func (c *Client) GetPendingPods(ctx context.Context, namespace string) ([]map[string]any, error) {
	pods, err := c.Clientset.CoreV1().Pods(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodPending {
			out = append(out, podMap(p))
		}
	}
	return out, nil
}

func (c *Client) GetCrashLoopPods(ctx context.Context, namespace string) ([]map[string]any, error) {
	pods, err := c.Clientset.CoreV1().Pods(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, p := range pods.Items {
		if podCrashLooping(p) {
			out = append(out, podMap(p))
		}
	}
	return out, nil
}

func (c *Client) GetNodeConditions(ctx context.Context) ([]map[string]any, error) {
	nodes, err := c.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		out = append(out, map[string]any{"name": n.Name, "uid": string(n.UID), "resourceVersion": n.ResourceVersion, "conditions": n.Status.Conditions})
	}
	return out, nil
}

func ns(namespace string) string {
	if namespace == "*" {
		return ""
	}
	return namespace
}
func podFailing(p corev1.Pod) bool {
	if p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodPending {
		return true
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && strings.Contains(strings.ToLower(cs.State.Waiting.Reason), "crash") {
			return true
		}
	}
	return false
}
func podCrashLooping(p corev1.Pod) bool {
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			return true
		}
	}
	return false
}

func base(name, namespace, uid, rv string, labels map[string]string) map[string]any {
	return map[string]any{"name": name, "namespace": namespace, "uid": uid, "resourceVersion": rv, "labels": labels}
}
func podMap(p corev1.Pod) map[string]any {
	m := base(p.Name, p.Namespace, string(p.UID), p.ResourceVersion, p.Labels)
	m["phase"] = p.Status.Phase
	m["nodeName"] = p.Spec.NodeName
	m["containers"] = len(p.Spec.Containers)
	m["containerStatuses"] = p.Status.ContainerStatuses
	return m
}
func nodeMap(n corev1.Node) map[string]any {
	m := base(n.Name, "", string(n.UID), n.ResourceVersion, n.Labels)
	m["ready"] = nodeReady(n)
	m["version"] = n.Status.NodeInfo.KubeletVersion
	return m
}
func deploymentMap(d appsv1.Deployment) map[string]any {
	m := base(d.Name, d.Namespace, string(d.UID), d.ResourceVersion, d.Labels)
	m["replicas"] = d.Status.Replicas
	m["availableReplicas"] = d.Status.AvailableReplicas
	return m
}
func serviceMap(s corev1.Service) map[string]any {
	m := base(s.Name, s.Namespace, string(s.UID), s.ResourceVersion, s.Labels)
	m["type"] = s.Spec.Type
	return m
}
func ingressMap(i networkingv1.Ingress) map[string]any {
	m := base(i.Name, i.Namespace, string(i.UID), i.ResourceVersion, i.Labels)
	m["class"] = i.Spec.IngressClassName
	return m
}
