# High Availability Guide

This guide explains how to run klag-exporter in high availability (HA) mode using Kubernetes leader election.

## Overview

By default, klag-exporter runs as a single instance. Running multiple instances without coordination results in duplicate Kafka API calls and inconsistent metrics. The HA feature enables **active-passive** deployment where:

- Only the **leader** instance collects metrics
- **Standby** instances wait idle, ready for failover
- Automatic failover occurs if the leader dies (typically 15-30 seconds)

## How It Works

klag-exporter uses the [kube-lease-manager](https://crates.io/crates/kube-lease-manager) crate to implement leader election via Kubernetes [Lease](https://kubernetes.io/docs/concepts/architecture/leases/) objects.

### Leader Election Flow

```
┌─────────────────┐     ┌─────────────────┐
│   Instance A    │     │   Instance B    │
│                 │     │                 │
│  ┌───────────┐  │     │  ┌───────────┐  │
│  │  Acquire  │  │     │  │  Acquire  │  │
│  │   Lease   │──┼─────┼──│   Lease   │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │
│        │        │     │        │        │
│        ▼        │     │        ▼        │
│   ┌─────────┐   │     │   ┌─────────┐   │
│   │ LEADER  │   │     │   │ STANDBY │   │
│   │         │   │     │   │         │   │
│   │ Collect │   │     │   │  Wait   │   │
│   │ metrics │   │     │   │         │   │
│   └─────────┘   │     │   └─────────┘   │
└─────────────────┘     └─────────────────┘
         │                       │
         ▼                       ▼
   ┌───────────┐          ┌───────────┐
   │ /metrics  │          │ /metrics  │
   │  (data)   │          │  (empty)  │
   │ /ready OK │          │/ready 503 │
   └───────────┘          └───────────┘
```

### Lease Mechanism

1. All instances compete for a Kubernetes Lease object
2. The winner becomes leader and starts collecting metrics
3. The leader periodically renews the lease (every `grace_period` seconds)
4. If the leader fails to renew (crash, network partition), the lease expires after `lease_duration`
5. Another instance acquires the lease and becomes the new leader

### Failover Timeline

```
t=0s    Leader crashes
t=5s    Lease renewal missed (grace_period)
t=15s   Lease expires (lease_duration)
t=15s   Standby acquires lease, becomes leader
t=16s   New leader starts collecting metrics
```

Default failover time: **15-20 seconds**

## Building with HA Support

The Kubernetes leader election feature is optional and requires the `kubernetes` feature flag:

```bash
# Build with HA support
cargo build --release --features kubernetes

# Build without HA (default, smaller binary)
cargo build --release
```

### Docker Build

```dockerfile
# Enable kubernetes feature in Docker build
RUN cargo build --release --features kubernetes
```

## Configuration

Add the `leadership` section to your configuration file:

```toml
[exporter]
poll_interval = "30s"
http_port = 8000

# Leader election configuration
[exporter.leadership]
enabled = true                              # Enable HA mode
lease_name = "klag-exporter"                # Kubernetes Lease resource name
lease_namespace = "${POD_NAMESPACE:-default}"  # Namespace (supports env vars)
lease_duration_secs = 15                    # Lease validity duration
grace_period_secs = 5                       # Renewal interval

[[clusters]]
name = "production"
bootstrap_servers = "kafka:9092"
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable leader election. When `false`, runs in single-instance mode. |
| `lease_name` | `"klag-exporter"` | Name of the Kubernetes Lease resource. |
| `lease_namespace` | `"default"` | Namespace for the Lease. Use `${POD_NAMESPACE}` for dynamic injection. |
| `identity` | Pod hostname | Unique identifier for this instance. Auto-detected from `HOSTNAME` or `POD_NAME` env vars. |
| `lease_duration_secs` | `15` | How long the lease is valid. Longer = slower failover but more stable. |
| `grace_period_secs` | `5` | How often to renew the lease. Must be less than `lease_duration_secs`. |

### Tuning Failover Time

The failover time is approximately `lease_duration_secs`:

| Use Case | lease_duration | grace_period | Failover Time |
|----------|---------------|--------------|---------------|
| Fast failover | 10s | 3s | ~10-13s |
| Balanced (default) | 15s | 5s | ~15-20s |
| Stable (flaky network) | 30s | 10s | ~30-40s |

**Warning**: Setting `lease_duration` too low can cause leader flapping on transient network issues.

## Kubernetes Setup

### 1. RBAC Configuration

klag-exporter needs permission to create/update Lease objects:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: klag-exporter
  namespace: monitoring
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: klag-exporter-leader-election
  namespace: monitoring
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: klag-exporter-leader-election
  namespace: monitoring
subjects:
- kind: ServiceAccount
  name: klag-exporter
  namespace: monitoring
roleRef:
  kind: Role
  name: klag-exporter-leader-election
  apiGroup: rbac.authorization.k8s.io
```

### 2. Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: klag-exporter
  namespace: monitoring
spec:
  replicas: 2  # Run multiple replicas for HA
  selector:
    matchLabels:
      app: klag-exporter
  template:
    metadata:
      labels:
        app: klag-exporter
    spec:
      serviceAccountName: klag-exporter
      containers:
      - name: klag-exporter
        image: ghcr.io/softwaremill/klag-exporter:latest
        ports:
        - containerPort: 8000
          name: http
        env:
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: http
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /etc/klag-exporter
      volumes:
      - name: config
        configMap:
          name: klag-exporter-config
```

### 3. ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: klag-exporter-config
  namespace: monitoring
data:
  config.toml: |
    [exporter]
    poll_interval = "30s"
    http_port = 8000

    [exporter.leadership]
    enabled = true
    lease_name = "klag-exporter"
    lease_namespace = "${POD_NAMESPACE}"

    [[clusters]]
    name = "production"
    bootstrap_servers = "kafka.kafka.svc.cluster.local:9092"
```

### 4. Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: klag-exporter
  namespace: monitoring
  labels:
    app: klag-exporter
spec:
  ports:
  - port: 8000
    targetPort: http
    name: http
  selector:
    app: klag-exporter
```

**Important**: The Service will route traffic to all pods, but only the leader returns metrics. Prometheus will scrape the leader (ready pod) thanks to the readiness probe.

### 5. ServiceMonitor (for Prometheus Operator)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: klag-exporter
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: klag-exporter
  endpoints:
  - port: http
    interval: 30s
    path: /metrics
```

## HTTP Endpoints

The HA feature modifies endpoint behavior:

| Endpoint | Leader | Standby |
|----------|--------|---------|
| `GET /metrics` | Returns all lag metrics | Returns empty (no data) |
| `GET /health` | `200 OK` | `200 OK` |
| `GET /ready` | `200 OK` | `503 Service Unavailable` |
| `GET /leader` | `{"is_leader": true}` | `{"is_leader": false}` |

### Why /ready Returns 503 on Standby

Kubernetes uses the readiness probe to determine which pods receive traffic. By returning 503 when on standby:

1. Prometheus only scrapes the leader pod
2. Load balancers route to the leader
3. No duplicate metrics are collected

### Checking Leader Status

```bash
# Check which instance is leader
kubectl exec -it deploy/klag-exporter -- curl -s localhost:8000/leader

# Output on leader:
{"is_leader":true}

# Output on standby:
{"is_leader":false}
```

## Monitoring the HA Setup

### Viewing Lease Status

```bash
# Check lease ownership
kubectl get lease klag-exporter -n monitoring -o yaml
```

Example output:
```yaml
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: klag-exporter
  namespace: monitoring
spec:
  holderIdentity: klag-exporter-7d4f8b9c6-abc12
  leaseDurationSeconds: 15
  renewTime: "2024-01-15T10:30:45.123456Z"
```

### Logs

Leader acquisition:
```
INFO klag_exporter::leadership::kubernetes: Acquired leadership identity="klag-exporter-7d4f8b9c6-abc12"
INFO klag_exporter::cluster::manager: Acquired leadership - starting collection cluster="production"
```

Standby mode:
```
INFO klag_exporter::leadership::kubernetes: On standby, waiting for leadership identity="klag-exporter-7d4f8b9c6-xyz99"
INFO klag_exporter::cluster::manager: Starting in standby mode - waiting for leadership cluster="production"
```

Failover:
```
INFO klag_exporter::cluster::manager: Lost leadership - pausing collection cluster="production"
INFO klag_exporter::leadership::kubernetes: Acquired leadership identity="klag-exporter-7d4f8b9c6-xyz99"
INFO klag_exporter::cluster::manager: Acquired leadership - starting collection cluster="production"
```

## Troubleshooting

### Both Instances Stuck in Standby

**Symptom**: Neither instance becomes leader, both show `is_leader: false`

**Causes**:
1. RBAC misconfiguration - check ServiceAccount and Role binding
2. Wrong namespace in config vs. where Lease should be created

**Debug**:
```bash
# Check if lease exists
kubectl get lease klag-exporter -n monitoring

# Check pod logs for errors
kubectl logs -l app=klag-exporter -n monitoring | grep -i "lease\|leader\|error"

# Verify RBAC
kubectl auth can-i get leases --as=system:serviceaccount:monitoring:klag-exporter -n monitoring
kubectl auth can-i create leases --as=system:serviceaccount:monitoring:klag-exporter -n monitoring
kubectl auth can-i update leases --as=system:serviceaccount:monitoring:klag-exporter -n monitoring
```

### Leader Flapping

**Symptom**: Leadership switches back and forth frequently

**Causes**:
1. `lease_duration_secs` too short
2. Network instability
3. Pod resource limits causing slowdowns

**Fix**: Increase lease duration:
```toml
[exporter.leadership]
lease_duration_secs = 30
grace_period_secs = 10
```

### Metrics Gap During Failover

**Symptom**: Prometheus shows gaps in metrics during failover

**This is expected**. During the failover window (lease expiration), no instance collects metrics. To minimize gaps:

1. Reduce `lease_duration_secs` (but not below 10s)
2. Ensure fast pod startup with proper resource allocation
3. Use Prometheus recording rules to handle short gaps

### Feature Not Compiled In

**Symptom**: Leader election enabled but logs show:
```
WARN Kubernetes leadership is enabled in config but the 'kubernetes' feature is not compiled in. Falling back to single-instance mode.
```

**Fix**: Rebuild with the kubernetes feature:
```bash
cargo build --release --features kubernetes
```

## Comparison with Other Approaches

| Approach | Failover Time | Complexity | External Deps |
|----------|--------------|------------|---------------|
| **Kubernetes Lease (this)** | 15-30s | Medium | None |
| Manual active-passive | Manual | Low | None |
| etcd leader election | 5-10s | High | etcd cluster |
| Prometheus HA only | N/A | Low | Thanos/Cortex |

The Kubernetes Lease approach provides a good balance of automatic failover, moderate complexity, and no external dependencies beyond Kubernetes itself.

## Security Considerations

1. **Least Privilege**: The RBAC rules grant only the minimum permissions needed (get/create/update on Leases)
2. **Namespace Isolation**: Use a dedicated namespace and limit Role to that namespace
3. **No Secrets**: Leader election doesn't require any secrets or credentials beyond the ServiceAccount token
