# Hive Agent Enterprise Architecture and Production Guide

## Refactored package structure

* `cmd/agent`: boot orchestration and local health endpoints.
* `internal/state`: explicit state machine: `PENDING_KEY`, `REGISTERED`, `CONNECTING`, `ONLINE`, `DEGRADED`, `OFFLINE`, `REVOKED`.
* `internal/registration`: bootstrap registration, explicit capability registration, heartbeat, inventory delta, token refresh, and credential rotation support.
* `internal/websocket`: the only V1 tunnel transport; outbound TLS WebSocket with JWT auth, compression, ping/pong, reconnect backoff, state tracking, and metrics.
* `internal/inventory`: informer-based delta synchronization using `SharedInformers`, cache sync, resourceVersion, and batched change delivery.
* `internal/commands`: HolmesGPT-compatible allowlisted remote Kubernetes tool framework.
* `internal/holmesprovider`: control-plane-side HolmesGPT provider contract for tool discovery, capability negotiation, cluster targeting, multi-cluster aggregation, result normalization, and audit records.
* `internal/kubernetes`: in-cluster client, health snapshots, and read-only command implementations.
* `internal/heartbeat`: 30-second UI-aligned cluster health heartbeat.
* `internal/telemetry`: Prometheus and OpenTelemetry wiring.
* `api/proto`: retained future gRPC contracts; no gRPC transport is run in V1.

## HolmesGPT provider package

`internal/holmesprovider` models the Hive Control Plane layer that exposes Hive Agents as first-class HolmesGPT remote tools. It includes an in-memory `ToolRegistry`, HolmesGPT-compatible `ToolDefinition` schemas, a dispatcher interface for sending commands to agents, aggregation across single-cluster, group, and all-cluster targets, normalized result envelopes, investigation context metadata, and mandatory audit records.

## HolmesGPT remote tool compatibility

The agent dynamically registers Kubernetes capabilities during startup equivalent to the HolmesGPT Kubernetes toolsets, including pods, logs, events, nodes, namespaces, workload health, restart analysis, failed workloads, pending pods, CrashLoopBackOff pods, node conditions, and resource usage.

The investigation workflow is:

1. User asks the Hive UI a question.
2. HolmesGPT in the Hive backend chooses one or more tools.
3. Hive Backend translates the selected tool into an agent command, such as `get_crashloop_pods`.
4. One or more cluster-scoped agents execute the command against Kubernetes.
5. Agents return machine-readable JSON only.
6. The control plane aggregates multi-cluster results and passes the dataset to HolmesGPT for reasoning.

Future non-Kubernetes toolsets such as Prometheus, Grafana, Loki, Tempo, OpenSearch, cloud providers, Cilium, Istio, ArgoCD, and FluxCD can be added by registering additional capability names and implementing new allowlisted executors without changing the tunnel architecture.

## Migration from previous implementation

* Removed local HolmesGPT execution from the agent.
* Removed `holmes ask`, local Holmes server assumptions, and AI/LLM runtime dependencies from the image.
* Replaced free-form `query` execution with structured HolmesGPT-compatible tool commands.
* Replaced periodic full inventory upload with informer-based delta synchronization.
* Reduced RBAC by removing batch jobs/cronjobs and retaining only UI-promised read-only resources.
* Changed heartbeat to report cluster health, node readiness, pod phases, workload counts, and agent state.

## API key lifecycle

The Hive Control Plane owns these UI lifecycle APIs:

* `POST /api/v1/agent-keys`
* `GET /api/v1/agent-keys`
* `DELETE /api/v1/agent-keys/{id}`
* `POST /api/v1/agent-keys/{id}/rotate`

The agent consumes rotation through the secure tunnel using `credential_rotation` envelopes and applies the new JWT without reinstall. If the key is revoked, the control plane sends `revoke` and the agent transitions to `REVOKED`.

## Resource usage targets

The agent is designed for typically `<50MB` RAM and `<2%` CPU by using:

* no embedded AI runtime;
* informer caches instead of repeated full list uploads;
* batched delta syncs;
* read-only HolmesGPT tool execution on demand;
* a single outbound tunnel and pooled REST client.

Benchmark strategy:

```bash
go test ./internal/... -bench=. -benchmem
kubectl top pod -n holmes -l app.kubernetes.io/name=hive-agent
```

## Security review checklist

* Confirm chart renders no Ingress, NodePort, or LoadBalancer.
* Confirm RBAC has only `get`, `list`, and `watch` verbs.
* Confirm no `secrets`, `pods/exec`, `pods/portforward`, or token creation resources are granted.
* Confirm the image contains only the static agent binary and no HolmesGPT/LLM runtime.
* Confirm WebSocket URLs use `wss://` in production and TLS minimum version is 1.3.

## Production validation and runbooks

Use `docs/operational-runbook.md` for startup validation, troubleshooting, upgrade validation, resource measurement, and release readiness checks. The validation tests under `internal/validation` statically enforce chart security properties, required UI values, deny-ingress behavior, bounded inventory delta sync, and WebSocket reliability hooks.
