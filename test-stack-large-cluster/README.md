# Large Cluster Test Stack — FD Exhaustion Reproduction

Test environment for reproducing file descriptor exhaustion issues with klag-exporter under high-scale scenarios.

Creates 500 topics x 3 partitions (1500 partitions) with 10 consumer groups and a low `ulimit` on the klag-exporter container to trigger "Too many open files" errors.

## System Requirements

- Docker with at least 8GB memory allocated (Kafka alone uses up to 4GB)
- Docker Compose v2+
- ~2GB free disk space for container images

## Quick Start

```bash
cd test-stack-large-cluster
docker-compose up -d --build
```

Wait for topic creation to finish:
```bash
docker logs -f lc-topic-creator
```

Watch for FD exhaustion errors:
```bash
docker logs -f lc-klag-exporter
```

With the default `KLAG_NOFILE_LIMIT=256`, you should see "Too many open files" errors within a few collection cycles.

## Configuration

Edit `.env` to adjust scenarios:

| Variable | Default | Description |
|---|---|---|
| `KLAG_NOFILE_LIMIT` | `256` | ulimit for klag-exporter (lower = faster failure) |
| `NUM_TOPICS` | `500` | Number of topics to create |
| `PARTITIONS_PER_TOPIC` | `3` | Partitions per topic |
| `NUM_CONSUMER_GROUPS` | `10` | Number of consumer groups |

klag-exporter settings (`poll_interval`, `max_concurrent_fetches`, etc.) are configured directly in `klag-config.toml`.

After editing, apply with:
```bash
docker-compose up -d
```

## Access Points

| Service | URL |
|---|---|
| klag-exporter metrics | http://localhost:8100/metrics |
| Prometheus | http://localhost:9190 |
| Grafana | http://localhost:3100 (admin/admin) |
| Kafka UI | http://localhost:8180 |
| OTel Collector | http://localhost:8988 (internal), http://localhost:8989 (exported metrics) |

All ports are offset from the existing `test-stack/` so both can run simultaneously.

## Cleanup

```bash
docker-compose down -v
```
