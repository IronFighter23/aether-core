# Deployment

Aether-Core's relay is a small Python process. Production deployment
boils down to: pick a port, run it behind a reverse proxy with TLS,
persist the ledger to durable storage, and tune the security limits.

This guide covers each of those steps with working configurations.

## What you ship

Aether-Core does not have its own production binary. You write a
small Python entry point that wires together a `MeshNode`, a
`ClientGateway`, and a `ChronoLedger` for your specific deployment.
The pattern is exactly the one in `run_demo.py`, minus the
development static-file server.

A minimal production launcher:

```python
# server.py
import asyncio
import logging
import os
import signal

from aether_core import ChronoLedger, ClientGateway, MeshNode
from aether_core._security import SecurityLimits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

LEDGER_PATH = os.environ.get("AETHER_LEDGER", "/var/lib/aether/prod.jsonl")
NODE_ID     = os.environ.get("AETHER_NODE_ID", "prod-1")
GATEWAY_BIND = os.environ.get("AETHER_GATEWAY_BIND", "127.0.0.1")
GATEWAY_PORT = int(os.environ.get("AETHER_GATEWAY_PORT", "8211"))
MESH_BIND   = os.environ.get("AETHER_MESH_BIND", "0.0.0.0")
MESH_PORT   = int(os.environ.get("AETHER_MESH_PORT", "8201"))

# Production-scale limits. Tune to your workload.
LIMITS = SecurityLimits(
    messages_per_second        = 500.0,
    messages_burst             = 1_000,
    max_connections_total      = 2_048,
    max_connections_per_source = 64,
    max_frame_bytes            = 256 * 1024,
    max_message_bytes          = 64 * 1024,
    max_value_bytes            = 32 * 1024,
    handshake_timeout_s        = 10.0,
)

async def main():
    ledger = ChronoLedger(LEDGER_PATH)
    placeholder = {}

    async def on_op(op, src):
        await ledger.on_op(op, src)
        gw = placeholder.get("gw")
        if gw is not None:
            await gw.on_op(op, src)

    mesh = MeshNode(NODE_ID, host=MESH_BIND, port=MESH_PORT,
                    on_op=on_op, limits=LIMITS)
    gw   = ClientGateway(mesh, host=GATEWAY_BIND, port=GATEWAY_PORT,
                         limits=LIMITS)
    placeholder["gw"] = gw

    await ledger.boot(mesh)
    await mesh.start()
    await gw.start()

    logging.info("Aether-Core relay ready. ledger=%s, gw=%s:%d, mesh=%s:%d",
                 LEDGER_PATH, GATEWAY_BIND, GATEWAY_PORT, MESH_BIND, MESH_PORT)

    # Connect to federated peers from env (comma-separated host:port).
    for peer in os.environ.get("AETHER_PEERS", "").split(","):
        peer = peer.strip()
        if not peer: continue
        host, port = peer.rsplit(":", 1)
        try:
            await mesh.connect_to(host, int(port))
            logging.info("federated peer: %s", peer)
        except Exception as e:
            logging.warning("failed to dial peer %s: %s", peer, e)

    # Run until interrupted.
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, stop.set)
    await stop.wait()

    logging.info("shutting down…")
    await gw.stop()
    await mesh.stop()
    await ledger.close()
    logging.info("clean exit.")

if __name__ == "__main__":
    asyncio.run(main())
```

Save as `server.py` at the repo root. Run with
`python server.py`. Now you're ready to put it behind a real
deployment.

## Reverse proxy + TLS

The gateway speaks plain WebSocket. TLS termination is your job —
that's standard practice for application servers. Two common
options:

### nginx

```nginx
# /etc/nginx/sites-available/aether
upstream aether_gateway {
    server 127.0.0.1:8211;
}

server {
    listen 443 ssl http2;
    server_name app.example.com;

    ssl_certificate     /etc/letsencrypt/live/app.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/app.example.com/privkey.pem;

    # Static assets (your built HTML/JS/CSS)
    root /var/www/aether-app;
    index index.html;

    # WebSocket gateway
    location /ws {
        proxy_pass http://aether_gateway;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 86400s;     # WebSockets need long timeouts
        proxy_send_timeout 86400s;
    }

    # Everything else: serve the static SPA
    location / {
        try_files $uri $uri/ /index.html;
    }
}

server {
    listen 80;
    server_name app.example.com;
    return 301 https://$host$request_uri;
}
```

In your client code, point at the proxied WebSocket URL:

```js
const aether = new Aether('wss://app.example.com/ws');
```

### Caddy

```caddyfile
# Caddyfile
app.example.com {
    root * /var/www/aether-app
    file_server
    encode gzip

    reverse_proxy /ws 127.0.0.1:8211
}
```

Caddy auto-provisions Let's Encrypt certificates. Recommended if you
don't already have an nginx setup.

## Systemd service

```ini
# /etc/systemd/system/aether-relay.service
[Unit]
Description=Aether-Core relay
After=network.target

[Service]
Type=simple
User=aether
Group=aether
WorkingDirectory=/opt/aether-core
ExecStart=/opt/aether-core/.venv/bin/python /opt/aether-core/server.py
Restart=always
RestartSec=5

# Environment
Environment=AETHER_LEDGER=/var/lib/aether/prod.jsonl
Environment=AETHER_NODE_ID=prod-1
Environment=AETHER_GATEWAY_BIND=127.0.0.1
Environment=AETHER_GATEWAY_PORT=8211
Environment=AETHER_MESH_BIND=0.0.0.0
Environment=AETHER_MESH_PORT=8201

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/aether
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd -r -s /sbin/nologin aether
sudo mkdir -p /var/lib/aether
sudo chown aether:aether /var/lib/aether
sudo systemctl daemon-reload
sudo systemctl enable --now aether-relay
sudo journalctl -fu aether-relay
```

## Docker

```dockerfile
# Dockerfile
FROM python:3.12-slim AS base
WORKDIR /app

# Install uv for faster dep resolution.
RUN pip install --no-cache-dir uv

COPY pyproject.toml ./
COPY aether_core/ ./aether_core/
COPY server.py ./

RUN uv pip install --system --no-cache .

RUN useradd -r -u 1000 aether \
 && mkdir -p /var/lib/aether \
 && chown -R aether:aether /var/lib/aether

USER aether
EXPOSE 8211 8201
VOLUME ["/var/lib/aether"]

ENV AETHER_LEDGER=/var/lib/aether/prod.jsonl \
    AETHER_GATEWAY_BIND=0.0.0.0 \
    AETHER_GATEWAY_PORT=8211 \
    AETHER_MESH_BIND=0.0.0.0 \
    AETHER_MESH_PORT=8201

CMD ["python", "server.py"]
```

```bash
docker build -t aether-relay .
docker run -d --name aether \
  -p 127.0.0.1:8211:8211 \
  -p 0.0.0.0:8201:8201 \
  -v aether-data:/var/lib/aether \
  --restart unless-stopped \
  aether-relay
```

The `127.0.0.1:8211` binding keeps the gateway accessible only to
the local reverse proxy. The mesh port (8201) is exposed publicly so
federated peers can dial in.

## Persisting the ledger

The ledger is a single growing JSONL file. Production checklist:

- **Put it on a durable volume.** A bind-mount, a Docker named
  volume, an EBS-backed disk — anything where `fsync` actually
  reaches stable storage. **Do not** put it on `tmpfs` or
  inside the container's writable layer.
- **Back it up.** A nightly `cp` to S3 is sufficient. The file is
  append-only, so incremental backup is trivial — copy any byte
  range past the last backed-up offset.
- **Run compaction periodically.** Without compaction, boot time
  grows linearly with total operation count.

### Nightly compaction (cron)

```bash
# /etc/cron.daily/aether-compact
#!/bin/bash
set -e

# 1. Stop the relay (or, more elegantly, signal it to do this itself).
systemctl stop aether-relay

# 2. Compact and rotate.
cd /opt/aether-core
.venv/bin/python -m aether_core.compact \
    /var/lib/aether/prod.jsonl --rotate

# 3. Restart.
systemctl start aether-relay

# 4. Archive the rotated original after a grace period.
find /var/lib/aether -name 'prod.jsonl.archived.*' -mtime +14 -delete
```

The `--rotate` flag archives the old ledger and starts a fresh
empty one alongside the snapshot. Boot then reads the snapshot
(fast) plus only ops written after the snapshot's max-stamp.

For zero-downtime compaction, see "Multi-node federation" below.

## Multi-node federation

Run multiple relays and connect them. They gossip every operation;
clients can talk to any of them.

### Two nodes, A ↔ B

Run two `server.py` instances. On node B, set:

```
AETHER_PEERS=node-a.example.com:8201
```

Node B will dial node A on boot. Once connected, every operation
they each receive propagates to the other within milliseconds (see
[BENCHMARKS.md](../BENCHMARKS.md) — a 5-node ring converges 100
writes in 63 ms).

### Three+ nodes

Each node lists every other node in `AETHER_PEERS`. Duplicate
connections are detected and dropped, so you can list all peers
on all nodes without worrying about which dials first.

### Behind a load balancer

Browsers can connect to any of N relays via a TCP/WebSocket-aware
load balancer (HAProxy, nginx with `stream` module, AWS NLB, etc.).
Sticky sessions are **not** required because every relay holds the
same state (CRDT convergence guarantees it). A user can flap between
relays without seeing any inconsistency.

### Zero-downtime compaction with federation

1. Take node A out of the load balancer rotation.
2. Stop A, compact A's ledger, restart A.
3. A connects to its federated peers, catches up via gossip.
4. Re-add A to the rotation.
5. Repeat for each node.

Total user-visible downtime: zero.

## Observability

Aether-Core uses the standard `logging` module. Production
deployments should:

- Capture stdout (systemd + journald, or Docker logs + your log
  shipper) — every operation logs at INFO, every refused/closed
  connection at WARNING.
- Watch the ledger file size (`du -h prod.jsonl`) — if it grows
  faster than expected, you have a runaway client.
- Watch the number of file descriptors (`lsof -p $(pidof python) |
  wc -l`) — should be roughly `client_count + 10`.
- Run `python -m aether_core.compact <ledger> --dry-run` periodically
  to see the would-be compaction ratio.

There is no built-in Prometheus/StatsD exporter. If you need one,
write a small wrapper around `gw.client_count`, `mesh.peer_ids`,
`ledger.written_count`. Pull requests welcome.

## Scaling guidance

| Scale | What works | What breaks |
|---|---|---|
| 1–50 concurrent users, one relay | Default settings | Nothing |
| 50–500 concurrent users, one relay | Raise `messages_per_second`, `max_connections_total`, `max_message_bytes` | Single-process Python GIL becomes the limit around 10k msg/s sustained |
| 500–5,000 users | Multiple relays + LB + federation | Per-client gateway broadcast cost grows linearly with connected clients (N²) |
| 5,000+ users | Shard by document/room, multiple relay clusters | Single-room scaling — there's no fan-out tree, every relay sends to every client |

The "good fit" range is dozens to hundreds of concurrent users per
relay. Past that, the engine's design assumptions (everyone sees
every key, fan-out is full broadcast) stop being efficient.

## Pre-deployment checklist

Before you ship:

- [ ] TLS terminating in front of the gateway
- [ ] Reverse proxy `proxy_read_timeout` ≥ 24h (WebSockets are long-lived)
- [ ] Ledger on durable storage (not tmpfs, not container layer)
- [ ] Nightly backup of the ledger file
- [ ] Nightly or weekly compaction job
- [ ] `SecurityLimits` tuned for actual workload
- [ ] `messages_per_second` raised above default 100 if your UI bursts
- [ ] `max_connections_per_source` raised if you expect office NAT pools
- [ ] Service runs as a non-root user
- [ ] Logs shipped somewhere queryable
- [ ] Federated peers (if any) reachable from the mesh port
- [ ] Mesh port either firewalled OR using a trusted peer auth proxy
- [ ] Browser app uses `wss://` (not `ws://`) in production

## See also

- [Concepts](concepts.md) — why federation works the way it does
- [SECURITY.md](../SECURITY.md) — what attacks are mitigated by default
- [BENCHMARKS.md](../BENCHMARKS.md) — real numbers to set capacity expectations
