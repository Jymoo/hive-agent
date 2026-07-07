# Hive Agent V1 Operational Runbook

## Release criteria checklist

1. UI-generated Helm command installs the chart with only `clusterName`, `apiEndpoint`, `apiKey`, and `provider` required.
2. Pod starts without manual Secret, ConfigMap, RBAC, kubeconfig, token, NodePort, LoadBalancer, or Ingress setup.
3. Agent registers with `POST /api/v1/agents/register` and receives `clusterId`, `jwt`, and `websocketUrl`.
4. Agent posts `POST /api/v1/agents/capabilities` with all HolmesGPT Kubernetes tool capabilities.
5. Agent opens a TLS WebSocket tunnel and reaches `ONLINE` state.
6. Heartbeats are sent every 30 seconds with top-level `state`, `clusterHealth`, `nodes`, `pods`, and `workloads` fields.
7. Inventory synchronization uses SharedInformers and sends deltas only.
8. HolmesGPT backend can invoke every registered tool and receives structured JSON only.
9. RBAC grants only `get`, `list`, and `watch`; it never grants secrets, exec, port-forward, writes, or token creation.
10. Reconnect, JWT rotation, endpoint rotation, and revocation handling work without pod reinstall.

## Startup verification

```bash
kubectl get pods -n holmes -l app.kubernetes.io/name=hive-agent
kubectl logs -n holmes -l app.kubernetes.io/name=hive-agent --tail=100
```

Expected log milestones:

* configuration loaded;
* cluster metadata discovered through `rest.InClusterConfig()`;
* cluster registered;
* capabilities registered;
* secure WebSocket tunnel connected;
* heartbeat and inventory delta loops started.

## Troubleshooting

### Invalid API key

Symptoms: registration fails with an HTTP 401/403 or validation error.

Actions:

* Generate a new key in the Hive UI.
* Upgrade the release with the new value: `helm upgrade hive-agent hive/hive-agent -n holmes --reuse-values --set apiKey="agt_..."`.

### Backend unavailable or DNS failure

Symptoms: tunnel reconnect warnings and `OFFLINE` state.

Actions:

* Verify `apiEndpoint` is reachable from the cluster.
* Verify DNS and egress policy allow outbound HTTPS/WebSocket traffic.
* No pod restart should be required; reconnect uses exponential backoff.

### JWT expired

Symptoms: backend sends credential rotation or closes the socket.

Actions:

* Confirm the backend emits `credential_rotation` with a replacement JWT.
* Agent applies the rotated token in memory and reconnects without reinstall.

### Kubernetes API unavailable

Symptoms: heartbeat enters `DEGRADED`, inventory deltas pause, tool invocations fail with structured errors.

Actions:

* Verify the Kubernetes API server is reachable from the pod.
* Verify the `hive-agent` ServiceAccount and ClusterRoleBinding exist.

## Upgrade guide

Use Helm upgrades and preserve values:

```bash
helm upgrade hive-agent hive/hive-agent \
  --namespace holmes \
  --reuse-values \
  --set image.tag="<new-version>"
```

Expected behavior:

* existing API key Secret is preserved unless explicitly changed;
* cluster identity is re-registered using the same cluster name and cluster UID;
* capabilities are re-posted after bootstrap;
* the new pod reconnects to the backend and returns to `ONLINE`.

## Validation commands

```bash
go test ./internal/capabilities ./internal/holmesprovider ./internal/state ./internal/validation
helm lint charts/hive-agent --set clusterName=test --set apiEndpoint=https://example.com --set apiKey=agt_test --set provider=aws
helm template hive-agent charts/hive-agent --namespace holmes --set clusterName=test --set apiEndpoint=https://example.com --set apiKey=agt_test --set provider=aws
```

## Resource validation

Typical target: memory below 50Mi and CPU below 2% of a node core during steady state.

```bash
kubectl top pod -n holmes -l app.kubernetes.io/name=hive-agent
kubectl describe pod -n holmes -l app.kubernetes.io/name=hive-agent
```

The chart defaults to low requests and bounded limits; large clusters are handled with informer watches and batched deltas rather than repeated full-state uploads.
