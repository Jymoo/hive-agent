package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
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
		out = append(out, statefulSetMap(s))
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
		out = append(out, daemonSetMap(d))
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

// ── New Resource Getters ────────────────────────────────────────────────────

func (c *Client) GetReplicaSets(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.AppsV1().ReplicaSets(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, rs := range items.Items {
		m := base(rs.Name, rs.Namespace, string(rs.UID), rs.ResourceVersion, rs.Labels)
		m["replicas"] = rs.Status.Replicas
		m["readyReplicas"] = rs.Status.ReadyReplicas
		m["availableReplicas"] = rs.Status.AvailableReplicas
		if rs.Spec.Selector != nil {
			m["selector"] = rs.Spec.Selector.MatchLabels
		}
		ownerRefs := make([]map[string]any, 0, len(rs.OwnerReferences))
		for _, ref := range rs.OwnerReferences {
			ownerRefs = append(ownerRefs, map[string]any{
				"apiVersion": ref.APIVersion,
				"kind":       ref.Kind,
				"name":       ref.Name,
				"uid":        string(ref.UID),
			})
		}
		m["ownerReferences"] = ownerRefs
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetEndpoints(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().Endpoints(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, ep := range items.Items {
		m := base(ep.Name, ep.Namespace, string(ep.UID), ep.ResourceVersion, ep.Labels)
		subsets := make([]map[string]any, 0, len(ep.Subsets))
		for _, s := range ep.Subsets {
			addrs := make([]string, 0, len(s.Addresses))
			for _, a := range s.Addresses {
				addrs = append(addrs, a.IP)
			}
			ports := make([]map[string]any, 0, len(s.Ports))
			for _, p := range s.Ports {
				ports = append(ports, map[string]any{"name": p.Name, "port": p.Port, "protocol": p.Protocol})
			}
			subsets = append(subsets, map[string]any{"addresses": addrs, "ports": ports})
		}
		m["subsets"] = subsets
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetNetworkPolicies(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.NetworkingV1().NetworkPolicies(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, np := range items.Items {
		m := base(np.Name, np.Namespace, string(np.UID), np.ResourceVersion, np.Labels)
		if np.Spec.PodSelector.MatchLabels != nil {
			m["podSelector"] = np.Spec.PodSelector.MatchLabels
		}
		m["policyTypes"] = np.Spec.PolicyTypes
		m["ingressRuleCount"] = len(np.Spec.Ingress)
		m["egressRuleCount"] = len(np.Spec.Egress)
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetPersistentVolumes(ctx context.Context) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, pv := range items.Items {
		m := base(pv.Name, "", string(pv.UID), pv.ResourceVersion, pv.Labels)
		m["phase"] = string(pv.Status.Phase)
		m["storageClassName"] = pv.Spec.StorageClassName
		m["accessModes"] = pv.Spec.AccessModes
		m["reclaimPolicy"] = string(pv.Spec.PersistentVolumeReclaimPolicy)
		if cap, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
			m["capacity"] = cap.String()
		}
		if pv.Spec.ClaimRef != nil {
			m["claimRef"] = map[string]any{
				"name":      pv.Spec.ClaimRef.Name,
				"namespace": pv.Spec.ClaimRef.Namespace,
			}
		}
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetStorageClasses(ctx context.Context) ([]map[string]any, error) {
	items, err := c.Clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, sc := range items.Items {
		m := base(sc.Name, "", string(sc.UID), sc.ResourceVersion, sc.Labels)
		m["provisioner"] = sc.Provisioner
		if sc.ReclaimPolicy != nil {
			m["reclaimPolicy"] = string(*sc.ReclaimPolicy)
		}
		if sc.VolumeBindingMode != nil {
			m["volumeBindingMode"] = string(*sc.VolumeBindingMode)
		}
		m["allowVolumeExpansion"] = sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetConfigMaps(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().ConfigMaps(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, cm := range items.Items {
		m := base(cm.Name, cm.Namespace, string(cm.UID), cm.ResourceVersion, cm.Labels)
		keys := make([]string, 0, len(cm.Data))
		for k := range cm.Data {
			keys = append(keys, k)
		}
		m["keys"] = keys
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetSecrets(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().Secrets(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, s := range items.Items {
		m := base(s.Name, s.Namespace, string(s.UID), s.ResourceVersion, s.Labels)
		m["type"] = string(s.Type)
		keys := make([]string, 0, len(s.Data))
		for k := range s.Data {
			keys = append(keys, k)
		}
		m["keys"] = keys
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetServiceAccounts(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().ServiceAccounts(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, sa := range items.Items {
		m := base(sa.Name, sa.Namespace, string(sa.UID), sa.ResourceVersion, sa.Labels)
		secrets := make([]string, 0, len(sa.Secrets))
		for _, s := range sa.Secrets {
			secrets = append(secrets, s.Name)
		}
		m["secrets"] = secrets
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetRoles(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.RbacV1().Roles(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, r := range items.Items {
		m := base(r.Name, r.Namespace, string(r.UID), r.ResourceVersion, r.Labels)
		m["rules"] = rbacRules(r.Rules)
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetClusterRoles(ctx context.Context) ([]map[string]any, error) {
	items, err := c.Clientset.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, r := range items.Items {
		m := base(r.Name, "", string(r.UID), r.ResourceVersion, r.Labels)
		m["rules"] = rbacRules(r.Rules)
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetRoleBindings(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.RbacV1().RoleBindings(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, rb := range items.Items {
		m := base(rb.Name, rb.Namespace, string(rb.UID), rb.ResourceVersion, rb.Labels)
		m["roleRef"] = map[string]any{"apiGroup": rb.RoleRef.APIGroup, "kind": rb.RoleRef.Kind, "name": rb.RoleRef.Name}
		m["subjects"] = rbacSubjects(rb.Subjects)
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetClusterRoleBindings(ctx context.Context) ([]map[string]any, error) {
	items, err := c.Clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, crb := range items.Items {
		m := base(crb.Name, "", string(crb.UID), crb.ResourceVersion, crb.Labels)
		m["roleRef"] = map[string]any{"apiGroup": crb.RoleRef.APIGroup, "kind": crb.RoleRef.Kind, "name": crb.RoleRef.Name}
		m["subjects"] = rbacSubjects(crb.Subjects)
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetResourceQuotas(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().ResourceQuotas(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, rq := range items.Items {
		m := base(rq.Name, rq.Namespace, string(rq.UID), rq.ResourceVersion, rq.Labels)
		hard := map[string]string{}
		for k, v := range rq.Spec.Hard {
			hard[string(k)] = v.String()
		}
		used := map[string]string{}
		for k, v := range rq.Status.Used {
			used[string(k)] = v.String()
		}
		m["hard"] = hard
		m["used"] = used
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetLimitRanges(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoreV1().LimitRanges(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, lr := range items.Items {
		m := base(lr.Name, lr.Namespace, string(lr.UID), lr.ResourceVersion, lr.Labels)
		limits := make([]map[string]any, 0, len(lr.Spec.Limits))
		for _, lim := range lr.Spec.Limits {
			entry := map[string]any{"type": string(lim.Type)}
			if len(lim.Max) > 0 {
				maxMap := map[string]string{}
				for k, v := range lim.Max {
					maxMap[string(k)] = v.String()
				}
				entry["max"] = maxMap
			}
			if len(lim.Min) > 0 {
				minMap := map[string]string{}
				for k, v := range lim.Min {
					minMap[string(k)] = v.String()
				}
				entry["min"] = minMap
			}
			limits = append(limits, entry)
		}
		m["limits"] = limits
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetHorizontalPodAutoscalers(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.AutoscalingV2().HorizontalPodAutoscalers(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, hpa := range items.Items {
		m := base(hpa.Name, hpa.Namespace, string(hpa.UID), hpa.ResourceVersion, hpa.Labels)
		m["minReplicas"] = hpa.Spec.MinReplicas
		m["maxReplicas"] = hpa.Spec.MaxReplicas
		m["currentReplicas"] = hpa.Status.CurrentReplicas
		m["desiredReplicas"] = hpa.Status.DesiredReplicas
		m["scaleTargetRef"] = map[string]any{
			"apiVersion": hpa.Spec.ScaleTargetRef.APIVersion,
			"kind":       hpa.Spec.ScaleTargetRef.Kind,
			"name":       hpa.Spec.ScaleTargetRef.Name,
		}
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetPodDisruptionBudgets(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.PolicyV1().PodDisruptionBudgets(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, pdb := range items.Items {
		m := base(pdb.Name, pdb.Namespace, string(pdb.UID), pdb.ResourceVersion, pdb.Labels)
		if pdb.Spec.MinAvailable != nil {
			m["minAvailable"] = pdb.Spec.MinAvailable.String()
		}
		if pdb.Spec.MaxUnavailable != nil {
			m["maxUnavailable"] = pdb.Spec.MaxUnavailable.String()
		}
		if pdb.Spec.Selector != nil {
			m["selector"] = pdb.Spec.Selector.MatchLabels
		}
		m["currentHealthy"] = pdb.Status.CurrentHealthy
		m["desiredHealthy"] = pdb.Status.DesiredHealthy
		m["disruptionsAllowed"] = pdb.Status.DisruptionsAllowed
		out = append(out, m)
	}
	return out, nil
}

func (c *Client) GetLeases(ctx context.Context, namespace string) ([]map[string]any, error) {
	items, err := c.Clientset.CoordinationV1().Leases(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(items.Items))
	for _, l := range items.Items {
		m := base(l.Name, l.Namespace, string(l.UID), l.ResourceVersion, l.Labels)
		if l.Spec.HolderIdentity != nil {
			m["holderIdentity"] = *l.Spec.HolderIdentity
		}
		if l.Spec.RenewTime != nil {
			m["renewTime"] = l.Spec.RenewTime.UTC().Format(time.RFC3339)
		}
		if l.Spec.LeaseDurationSeconds != nil {
			m["leaseDurationSeconds"] = *l.Spec.LeaseDurationSeconds
		}
		out = append(out, m)
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

// ── Map Functions (topology-complete) ──────────────────────────────────────

func podMap(p corev1.Pod) map[string]any {
	m := base(p.Name, p.Namespace, string(p.UID), p.ResourceVersion, p.Labels)
	m["phase"] = p.Status.Phase
	m["nodeName"] = p.Spec.NodeName
	m["hostIP"] = p.Status.HostIP
	m["podIP"] = p.Status.PodIP

	// Full container specs (env, args, command, resources, image)
	containers := make([]map[string]any, 0, len(p.Spec.Containers))
	for _, c := range p.Spec.Containers {
		env := make([]map[string]any, 0, len(c.Env))
		for _, e := range c.Env {
			env = append(env, map[string]any{"name": e.Name, "value": e.Value})
		}
		ports := make([]map[string]any, 0, len(c.Ports))
		for _, port := range c.Ports {
			ports = append(ports, map[string]any{"containerPort": port.ContainerPort, "protocol": port.Protocol, "name": port.Name})
		}
		containers = append(containers, map[string]any{
			"name":    c.Name,
			"image":   c.Image,
			"command": c.Command,
			"args":    c.Args,
			"env":     env,
			"ports":   ports,
			"resources": map[string]any{
				"requests": quantityMapStr(c.Resources.Requests),
				"limits":   quantityMapStr(c.Resources.Limits),
			},
		})
	}
	m["spec"] = map[string]any{
		"nodeName":       p.Spec.NodeName,
		"serviceAccount": p.Spec.ServiceAccountName,
		"containers":     containers,
	}
	m["containerStatuses"] = p.Status.ContainerStatuses

	// ownerReferences — critical for Pod→ReplicaSet→Deployment chain
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

func nodeMap(n corev1.Node) map[string]any {
	m := base(n.Name, "", string(n.UID), n.ResourceVersion, n.Labels)
	m["ready"] = nodeReady(n)
	m["version"] = n.Status.NodeInfo.KubeletVersion

	// Full conditions
	conditions := make([]map[string]any, 0, len(n.Status.Conditions))
	for _, c := range n.Status.Conditions {
		conditions = append(conditions, map[string]any{
			"type":    string(c.Type),
			"status":  string(c.Status),
			"reason":  c.Reason,
			"message": c.Message,
		})
	}
	m["conditions"] = conditions
	m["capacity"] = quantityMapStr(n.Status.Capacity)
	m["allocatable"] = quantityMapStr(n.Status.Allocatable)
	m["nodeInfo"] = map[string]any{
		"kubeletVersion":          n.Status.NodeInfo.KubeletVersion,
		"osImage":                 n.Status.NodeInfo.OSImage,
		"containerRuntimeVersion": n.Status.NodeInfo.ContainerRuntimeVersion,
		"kernelVersion":           n.Status.NodeInfo.KernelVersion,
		"operatingSystem":         n.Status.NodeInfo.OperatingSystem,
		"architecture":            n.Status.NodeInfo.Architecture,
	}

	taints := make([]map[string]any, 0, len(n.Spec.Taints))
	for _, t := range n.Spec.Taints {
		taints = append(taints, map[string]any{"key": t.Key, "value": t.Value, "effect": string(t.Effect)})
	}
	m["taints"] = taints
	m["unschedulable"] = n.Spec.Unschedulable

	return m
}

func deploymentMap(d appsv1.Deployment) map[string]any {
	m := base(d.Name, d.Namespace, string(d.UID), d.ResourceVersion, d.Labels)
	m["replicas"] = d.Status.Replicas
	m["availableReplicas"] = d.Status.AvailableReplicas
	m["readyReplicas"] = d.Status.ReadyReplicas
	m["updatedReplicas"] = d.Status.UpdatedReplicas
	m["unavailableReplicas"] = d.Status.UnavailableReplicas

	// spec.selector.matchLabels — critical for Service→Deployment matching
	if d.Spec.Selector != nil {
		m["selector"] = d.Spec.Selector.MatchLabels
	}

	// Pod template labels (for label-based matching)
	m["templateLabels"] = d.Spec.Template.Labels

	// Container specs for env-var dependency scanning
	containers := make([]map[string]any, 0, len(d.Spec.Template.Spec.Containers))
	for _, c := range d.Spec.Template.Spec.Containers {
		env := make([]map[string]any, 0, len(c.Env))
		for _, e := range c.Env {
			env = append(env, map[string]any{"name": e.Name, "value": e.Value})
		}
		containers = append(containers, map[string]any{
			"name":  c.Name,
			"image": c.Image,
			"env":   env,
		})
	}
	m["containers"] = containers

	// ownerReferences
	ownerRefs := make([]map[string]any, 0, len(d.OwnerReferences))
	for _, ref := range d.OwnerReferences {
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

func serviceMap(s corev1.Service) map[string]any {
	m := base(s.Name, s.Namespace, string(s.UID), s.ResourceVersion, s.Labels)
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
			"nodePort":   p.NodePort,
		})
	}
	m["ports"] = ports
	m["externalIPs"] = s.Spec.ExternalIPs

	return m
}

func ingressMap(i networkingv1.Ingress) map[string]any {
	m := base(i.Name, i.Namespace, string(i.UID), i.ResourceVersion, i.Labels)
	m["class"] = i.Spec.IngressClassName

	// spec.rules — critical for Ingress→Service edge construction
	rules := make([]map[string]any, 0, len(i.Spec.Rules))
	for _, r := range i.Spec.Rules {
		paths := []map[string]any{}
		if r.HTTP != nil {
			for _, p := range r.HTTP.Paths {
				svcName := ""
				svcPort := 0
				if p.Backend.Service != nil {
					svcName = p.Backend.Service.Name
					svcPort = int(p.Backend.Service.Port.Number)
				}
				paths = append(paths, map[string]any{
					"path":        p.Path,
					"pathType":    string(*p.PathType),
					"serviceName": svcName,
					"servicePort": svcPort,
				})
			}
		}
		rules = append(rules, map[string]any{
			"host":  r.Host,
			"paths": paths,
		})
	}
	m["rules"] = rules

	tlsHosts := []string{}
	for _, t := range i.Spec.TLS {
		tlsHosts = append(tlsHosts, t.Hosts...)
	}
	m["tlsHosts"] = tlsHosts

	return m
}

func statefulSetMap(s appsv1.StatefulSet) map[string]any {
	m := base(s.Name, s.Namespace, string(s.UID), s.ResourceVersion, s.Labels)
	m["replicas"] = s.Status.Replicas
	m["readyReplicas"] = s.Status.ReadyReplicas
	m["currentReplicas"] = s.Status.CurrentReplicas
	m["updatedReplicas"] = s.Status.UpdatedReplicas

	if s.Spec.Selector != nil {
		m["selector"] = s.Spec.Selector.MatchLabels
	}
	m["templateLabels"] = s.Spec.Template.Labels

	ownerRefs := make([]map[string]any, 0, len(s.OwnerReferences))
	for _, ref := range s.OwnerReferences {
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

func daemonSetMap(d appsv1.DaemonSet) map[string]any {
	m := base(d.Name, d.Namespace, string(d.UID), d.ResourceVersion, d.Labels)
	m["desiredNumberScheduled"] = d.Status.DesiredNumberScheduled
	m["numberReady"] = d.Status.NumberReady
	m["numberAvailable"] = d.Status.NumberAvailable
	m["numberUnavailable"] = d.Status.NumberUnavailable

	if d.Spec.Selector != nil {
		m["selector"] = d.Spec.Selector.MatchLabels
	}
	m["templateLabels"] = d.Spec.Template.Labels

	ownerRefs := make([]map[string]any, 0, len(d.OwnerReferences))
	for _, ref := range d.OwnerReferences {
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

// ── Helpers ─────────────────────────────────────────────────────────────────

func quantityMapStr(rm corev1.ResourceList) map[string]string {
	out := make(map[string]string, len(rm))
	for k, v := range rm {
		out[string(k)] = v.String()
	}
	return out
}

func rbacRules(rules []rbacv1.PolicyRule) []map[string]any {
	out := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		out = append(out, map[string]any{
			"verbs":         r.Verbs,
			"apiGroups":     r.APIGroups,
			"resources":     r.Resources,
			"resourceNames": r.ResourceNames,
		})
	}
	return out
}

func rbacSubjects(subjects []rbacv1.Subject) []map[string]any {
	out := make([]map[string]any, 0, len(subjects))
	for _, s := range subjects {
		out = append(out, map[string]any{
			"kind":      s.Kind,
			"name":      s.Name,
			"namespace": s.Namespace,
			"apiGroup":  s.APIGroup,
		})
	}
	return out
}

// ── PVC / Job / CronJob (existing, preserved) ───────────────────────────────

func (c *Client) GetPVCs(ctx context.Context, namespace string) ([]map[string]any, error) {
	pvcs, err := c.Clientset.CoreV1().PersistentVolumeClaims(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(pvcs.Items))
	for _, pvc := range pvcs.Items {
		out = append(out, pvcMap(pvc))
	}
	return out, nil
}

func pvcMap(pvc corev1.PersistentVolumeClaim) map[string]any {
	m := base(pvc.Name, pvc.Namespace, string(pvc.UID), pvc.ResourceVersion, pvc.Labels)
	m["phase"] = string(pvc.Status.Phase)
	storageClassName := ""
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}
	m["storageClassName"] = storageClassName
	m["volumeName"] = pvc.Spec.VolumeName
	storageCapacity := ""
	if pvc.Status.Capacity != nil {
		if cap, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
			storageCapacity = cap.String()
		}
	}
	m["capacity"] = storageCapacity
	m["accessModes"] = pvc.Spec.AccessModes
	return m
}

func (c *Client) GetJobs(ctx context.Context, namespace string) ([]map[string]any, error) {
	jobs, err := c.Clientset.BatchV1().Jobs(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(jobs.Items))
	for _, job := range jobs.Items {
		out = append(out, jobMap(job))
	}
	return out, nil
}

func jobMap(j batchv1.Job) map[string]any {
	m := base(j.Name, j.Namespace, string(j.UID), j.ResourceVersion, j.Labels)
	m["completions"] = 1
	if j.Spec.Completions != nil {
		m["completions"] = *j.Spec.Completions
	}
	m["succeeded"] = j.Status.Succeeded
	m["active"] = j.Status.Active
	m["failed"] = j.Status.Failed
	m["selector"] = j.Spec.Selector
	ownerRefs := make([]map[string]any, 0, len(j.OwnerReferences))
	for _, ref := range j.OwnerReferences {
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

func (c *Client) GetCronJobs(ctx context.Context, namespace string) ([]map[string]any, error) {
	cjs, err := c.Clientset.BatchV1().CronJobs(ns(namespace)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(cjs.Items))
	for _, cj := range cjs.Items {
		out = append(out, cronJobMap(cj))
	}
	return out, nil
}

func cronJobMap(cj batchv1.CronJob) map[string]any {
	m := base(cj.Name, cj.Namespace, string(cj.UID), cj.ResourceVersion, cj.Labels)
	m["schedule"] = cj.Spec.Schedule
	m["selector"] = cj.Spec.JobTemplate.Spec.Selector
	if cj.Status.LastScheduleTime != nil {
		m["lastScheduleTime"] = cj.Status.LastScheduleTime.Time.Format(time.RFC3339)
	}
	m["active"] = len(cj.Status.Active)
	m["suspend"] = cj.Spec.Suspend != nil && *cj.Spec.Suspend
	return m
}

// Ensure unused imports are referenced to satisfy the compiler.
var _ = autoscalingv2.HorizontalPodAutoscaler{}
var _ = policyv1.PodDisruptionBudget{}
var _ = rbacv1.Role{}
var _ = storagev1.StorageClass{}
