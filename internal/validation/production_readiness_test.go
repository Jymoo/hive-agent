package validation

import (
	"os"
	"strings"
	"testing"
)

func read(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestHelmChartRequiresOnlyUIValues(t *testing.T) {
	values := read(t, "../../charts/hive-agent/values.yaml")
	for _, key := range []string{"clusterName:", "apiEndpoint:", "apiKey:", "provider:"} {
		if !strings.Contains(values, key) {
			t.Fatalf("values.yaml missing required UI key %s", key)
		}
	}
	configMap := read(t, "../../charts/hive-agent/templates/configmap.yaml")
	secret := read(t, "../../charts/hive-agent/templates/secret.yaml")
	for _, required := range []string{"clusterName is required", "apiEndpoint is required", "provider is required"} {
		if !strings.Contains(configMap, required) {
			t.Fatalf("configmap template missing required validation %q", required)
		}
	}
	if !strings.Contains(secret, "apiKey is required") {
		t.Fatal("secret template must require apiKey")
	}
}

func TestHelmChartHasNoInboundExposure(t *testing.T) {
	templates := read(t, "../../charts/hive-agent/templates/deployment.yaml") + read(t, "../../charts/hive-agent/templates/networkpolicy.yaml")
	for _, forbidden := range []string{"kind: Ingress", "type: NodePort", "type: LoadBalancer"} {
		if strings.Contains(templates, forbidden) {
			t.Fatalf("chart must not render inbound exposure %q", forbidden)
		}
	}
	if !strings.Contains(templates, "ingress: []") {
		t.Fatal("network policy must deny ingress")
	}
	if !strings.Contains(templates, "command: [\"/usr/local/bin/hive-agent\", \"readyz\"]") || !strings.Contains(templates, "command: [\"/usr/local/bin/hive-agent\", \"healthz\"]") {
		t.Fatal("probes must use exec checks so the deny-ingress network policy does not break kubelet probes")
	}
}

func TestRBACIsReadOnlyAndExcludesForbiddenResources(t *testing.T) {
	rbac := read(t, "../../charts/hive-agent/templates/rbac.yaml")
	for _, verb := range []string{"create", "update", "patch", "delete", "deletecollection"} {
		if strings.Contains(rbac, "\""+verb+"\"") {
			t.Fatalf("rbac must not grant write verb %s", verb)
		}
	}
	for _, resource := range []string{"secrets", "pods/exec", "pods/portforward", "serviceaccounts/token"} {
		if strings.Contains(rbac, resource) {
			t.Fatalf("rbac must not grant forbidden resource %s", resource)
		}
	}
	for _, verb := range []string{"\"get\"", "\"list\"", "\"watch\""} {
		if !strings.Contains(rbac, verb) {
			t.Fatalf("rbac missing read verb %s", verb)
		}
	}
}

func TestWebSocketReliabilityHooksArePresent(t *testing.T) {
	manager := read(t, "../../internal/websocket/manager.go")
	for _, required := range []string{"NewExponentialBackOff", "SetPongHandler", "credential_rotation", "revoke", "TunnelReconnects", "TunnelURL()"} {
		if !strings.Contains(manager, required) {
			t.Fatalf("websocket manager missing reliability hook %q", required)
		}
	}
}

func TestInventoryUsesBoundedDeltaSync(t *testing.T) {
	inventory := read(t, "../../internal/inventory/inventory.go")
	for _, required := range []string{"NewSharedInformerFactory", "ResourceVersion", "make(chan registration.InventoryEvent, 4096)", "len(batch) >= 200", "InventoryDelta"} {
		if !strings.Contains(inventory, required) {
			t.Fatalf("inventory implementation missing delta/scale property %q", required)
		}
	}
}
