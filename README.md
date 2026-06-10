# Swarm DNS Proxy

Dynamic DNS proxy that maps Docker Swarm service replicas to predictable FQDNs via configurable templates.

Watches Docker Swarm through the socket and automatically maintains DNS A-records
for every replica based on a user-defined FQDN template.

---

## Problem

In Docker Swarm, replica IP addresses change on every deploy. The built-in Swarm DNS
resolves only the service name → VIP (a single address) or DNSRR (a list with no way
to identify a specific replica). There is no way to address a **specific replica**
by a predictable name.

## Solution

Swarm DNS Proxy polls the Docker API, builds FQDNs from your template, and runs
its own DNS server with up-to-date records.

---

## Quick Start

```bash
# 1. Build
docker build -t swarm-dns-proxy .

# 2. Deploy to Swarm
docker stack deploy -c docker-compose.yaml dns-proxy

# 3. Test
dig @<manager-ip> -p 5353 worker-1.replica-001.nginx.swarm.local

# 4. HTTP API — list all current records
curl http://<manager-ip>:8053/records
```

---

## Example Stack

You can find a complete working example inside the [example](./example) directory. It contains a simple Hello World setup (using a sticky hello application and Nginx ingress) which demonstrates how to configure Nginx dynamic upstreams with a custom DNS resolver. You can use it as a template or reference analogy for your own microservices.

---

## FQDN Template

Template variables:

| Variable    | Description                                      | Example        |
|-------------|--------------------------------------------------|----------------|
| `{node}`    | Swarm node hostname                              | `worker-1`     |
| `{service}` | Service name                                     | `nginx`        |
| `{stack}`   | Stack namespace (label) or `default`             | `myapp`        |
| `{slot}`    | Replica slot number (1, 2, 3…)                   | `2`            |
| `{replica}` | Zero-padded slot (001, 002…)                     | `002`          |
| `{task_id}` | Short task ID (12 characters)                    | `a1b2c3d4e5f6` |
| `{network}` | Overlay network name                             | `my-overlay`   |

### Template Examples

```yaml
# Standard — node + replica + service
template: "{node}.replica-{replica}.{service}.swarm"
# → worker-1.replica-001.nginx.swarm

# With stack
template: "{replica}.{service}.{stack}.internal"
# → 001.api.myapp.internal

# With network (useful with multiple overlays)
template: "{node}.{service}.{network}.dc"
# → worker-2.redis.backend.dc.local

# By task ID (guaranteed unique)
template: "{task_id}.{service}.swarm"
# → a1b2c3d4e5f6.nginx.swarm
```

---

## Network Mode Handling

### VIP (default)

With endpoint mode `vip`, Swarm assigns a virtual IP to the service and
load-balances traffic internally. **Swarm DNS Proxy fetches the actual container
IPs from the task's NetworksAttachments**, bypassing the VIP. Each replica gets
its own DNS record.

### DNSRR

With `endpoint_mode: dnsrr`, Swarm does not create a VIP and instead returns all
IPs via round-robin. The proxy works the same way — it extracts each task's IP from
NetworksAttachments and creates an individual FQDN record.

### Host Mode

When ports are published with `mode: host`, the container is bound to the node's
network stack. The proxy uses the **node's own IP** (from `node.Status.Addr`) and
maps it to the replica's FQDN.

---

## Configuration

### `config.yaml`

```yaml
template: "{node}.replica-{replica}.{service}.swarm.local"
dns_port: 53
ttl: 30
poll_interval: 5
docker_socket: "/var/run/docker.sock"
upstream_dns: "8.8.8.8"
upstream_port: 53
log_level: "INFO"

# Service filter (optional).
# When specified, ONLY the listed services are tracked.
services:
  - name: nginx
    template: "{node}.{replica}.nginx.internal"
    networks:
      - frontend

  - name: api
    networks:
      - backend
```

### Environment Variables

Environment variables **override** values from the config file:

| Variable        | Default                            |
|-----------------|------------------------------------|
| `CONFIG_PATH`   | `/etc/swarm-dns-proxy/config.yaml` |
| `DNS_TEMPLATE`  | (from file)                        |
| `DNS_PORT`      | `53`                               |
| `DNS_TTL`       | `30`                               |
| `POLL_INTERVAL` | `5`                                |
| `DOCKER_SOCKET` | `/var/run/docker.sock`             |
| `UPSTREAM_DNS`  | `8.8.8.8`                          |
| `UPSTREAM_PORT` | `53`                               |
| `LOG_LEVEL`     | `INFO`                             |
| `HTTP_PORT`     | `8053`                             |

---

## HTTP API

| Endpoint       | Description                     |
|----------------|---------------------------------|
| `GET /health`  | `{"status": "ok"}`              |
| `GET /records` | List all current DNS records    |

Example `/records` response:

```json
{
  "records": [
    {"fqdn": "worker-1.replica-001.nginx.swarm", "ip": "10.0.1.5"},
    {"fqdn": "worker-2.replica-002.nginx.swarm", "ip": "10.0.1.6"}
  ],
  "count": 2
}
```

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Swarm DNS Proxy                    │
│                                                 │
│  ┌──────────────┐    ┌──────────────────────┐   │
│  │ SwarmWatcher  │───▶│  DNS Record Table    │   │
│  │  (poll loop)  │    │  fqdn → [ip, …]      │   │
│  └──────┬───────┘    └──────────┬───────────┘   │
│         │                       │               │
│         ▼                       ▼               │
│  ┌──────────────┐    ┌──────────────────────┐   │
│  │ Docker Socket │    │   UDP DNS Server     │   │
│  │  /services    │    │   (port 53/5353)     │   │
│  │  /tasks       │    │                      │   │
│  │  /nodes       │    │  match? → respond    │   │
│  │  /networks    │    │  miss?  → upstream   │   │
│  └──────────────┘    └──────────────────────┘   │
│                                                 │
│         ┌──────────────────────┐                │
│         │   HTTP API (:8053)   │                │
│         │  /health  /records   │                │
│         └──────────────────────┘                │
└─────────────────────────────────────────────────┘
```

---

## Using as a Node Resolver

To make all containers on a node use Swarm DNS Proxy, add to
`/etc/docker/daemon.json`:

```json
{
  "dns": ["<proxy-host-ip>"]
}
```

Or specify DNS in the compose file of a specific service:

```yaml
services:
  myapp:
    dns:
      - <proxy-host-ip>
```

---

## Limitations and Future Improvements

- **A-records only** — AAAA (IPv6), SRV, and PTR are not yet supported.
- **Polling** — the Docker API is polled every N seconds. Can be switched to the Docker Events API (`/events?filters=...`) for instant updates.
- **Single instance** — no DNS table replication between multiple proxies. For HA, run one on each manager node + keepalived/VIP.
- **No TCP DNS** — only UDP is implemented. A TCP fallback is needed for responses exceeding 512 bytes.

---

## License

MIT
