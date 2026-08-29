# Running NCloud from Scratch

Two ways to run the system: **Local** (build and run directly on your machine) or **Docker** (containerised, no build toolchain needed).

---

## Prerequisites

### Local
| Tool | Version | Notes |
|---|---|---|
| g++ | ≥ 9 | C++17 support required |
| make | any | |
| libcurl | dev headers | `apt install libcurl4-openssl-dev` / `brew install curl` |
| netcat | any | for the health-check in `ncloud_control.sh` |

### Docker
| Tool | Version |
|---|---|
| Docker Engine | ≥ 20 |
| Docker Compose | v2 (`docker compose`) |

---

## Option 1 — Local (bare metal)

### 1. Build everything

```bash
cd ncloud_t05
make
```

This builds five binaries:

| Binary | Location |
|---|---|
| `coordinator` | `coordinator/coordinator` |
| `kvserver` | `kvstore/kvserver` |
| `feserver` | `frontend/feserver` |
| `smtp_server` | `smtp_server/smtp_server` |
| `frontend_lb` | `frontend_lb/frontend_lb` |

---

### 2. Start the full cluster (recommended)

The `ncloud_control.sh` script manages every process. Run each command in a separate terminal, **in this order**:

```bash
# Step 1 — coordinator (port 7110)
./ncloud_control.sh coordinator start

# Step 2 — four KV backend nodes (ports 7500-7503, repl 7600-7603)
./ncloud_control.sh backend start node1
./ncloud_control.sh backend start node2
./ncloud_control.sh backend start node3
./ncloud_control.sh backend start node4

# Step 3 — three frontend servers (ports 8090-8092)
./ncloud_control.sh frontend start fe1
./ncloud_control.sh frontend start fe2
./ncloud_control.sh frontend start fe3

# Step 4 — load balancer (port 8088)
./ncloud_control.sh lb start

# Step 5 — SMTP server (port 2525)
./ncloud_control.sh smtp start
```

Open your browser at **http://127.0.0.1:8088**
Admin console: **http://127.0.0.1:8088/admin**

---

### 3. Stop the cluster

```bash
./ncloud_control.sh coordinator stop
./ncloud_control.sh backend stop node1
./ncloud_control.sh backend stop node2
./ncloud_control.sh backend stop node3
./ncloud_control.sh backend stop node4
./ncloud_control.sh frontend stop fe1
./ncloud_control.sh frontend stop fe2
./ncloud_control.sh frontend stop fe3
./ncloud_control.sh lb stop
./ncloud_control.sh smtp stop
```

---

### 4. Restart a single component (fault-tolerance demo)

```bash
# Kill node2 and bring it back — coordinator will detect the failure and promote a secondary
./ncloud_control.sh backend stop node2
./ncloud_control.sh backend start node2
```

The same pattern works for any frontend or the coordinator itself.

---

### 5. Minimal single-node setup (dev/testing only)

If you only want to test the KV store and frontend without full replication:

```bash
mkdir -p /tmp/pc_data
./kvstore/kvserver --port 5000 --data /tmp/pc_data --tablet main &
./frontend/feserver --port 8080 --kv-host 127.0.0.1 --kv-port 5000 --id fe1 &
```

Open browser at **http://127.0.0.1:8080** (no load balancer, no replication).

---

### Port reference (local)

| Service | Port |
|---|---|
| Load balancer (main entry point) | 8088 |
| Frontend fe1 / fe2 / fe3 | 8090 / 8091 / 8092 |
| Coordinator | 7110 |
| KV node1 / node2 / node3 / node4 | 7500 / 7501 / 7502 / 7503 |
| Replication node1–node4 | 7600 / 7601 / 7602 / 7603 |
| SMTP inbound | 2525 |

---

## Option 2 — Docker

### 1. Build the image

```bash
cd ncloud_t05
docker build -t ncloud .
```

The Dockerfile uses a two-stage build: Ubuntu 22.04 builder compiles all binaries, then a lean runtime image is produced.

---

### 2. Start the full cluster

```bash
docker compose up
```

Docker Compose starts all services in dependency order (coordinator → KV nodes → frontends → load balancer + SMTP) and runs health checks between each layer.

Open your browser at **http://localhost:8088**
Admin console: **http://localhost:8088/admin**

Run in the background with:

```bash
docker compose up -d
```

---

### 3. Stop the cluster

```bash
docker compose down
```

To also delete persistent KV data volumes:

```bash
docker compose down -v
```

---

### 4. Restart a single container (fault-tolerance demo)

```bash
# Kill node2 — coordinator detects the failure within ~1.5 seconds
docker compose stop node2

# Bring it back — it will resync from the primary
docker compose start node2
```

---

### 5. Optional — outbound email relay (SMTP)

To send email to external addresses, create a `.smtp.env` file from the provided example:

```bash
cp smtp.env.example .smtp.env
```

Edit `.smtp.env` and fill in your Gmail credentials (requires a
[Gmail App Password](https://support.google.com/accounts/answer/185833)):

```
SMTP_MODE=relay
SMTP_RELAY_HOST=smtp.gmail.com
SMTP_RELAY_PORT=587
SMTP_RELAY_USER=you@gmail.com
SMTP_RELAY_PASS=xxxx xxxx xxxx xxxx   # 16-char app password
SMTP_RELAY_FROM=you@gmail.com
```

The SMTP container picks up `.smtp.env` automatically on next `docker compose up`.
Without it, inbound mail between local NCloud users still works; only external relay is disabled.

---

### Port reference (Docker)

Only two ports are exposed to the host:

| Service | Host port | Container port |
|---|---|---|
| Load balancer (main entry point) | 8088 | 8088 |
| SMTP inbound | 2525 | 2525 |

All other services (KV nodes, frontends, coordinator) communicate on the internal `ncloud` Docker network and are not reachable from the host directly.

---

## Tablet / sharding layout

The default configuration splits the keyspace into four tablets across four nodes:

| Tablet | Row-key range | Primary candidates |
|---|---|---|
| tabletA | `a` – `g` | node1, node2, node3 |
| tabletB | `g` – `m` | node2, node3, node4 |
| tabletC | `m` – `s` | node3, node4, node1 |
| tabletD | `s` – end | node4, node1, node2 |

Each tablet has 3 replicas. The coordinator automatically elects a primary for each tablet and handles failover.

---

## Troubleshooting

**Port already in use**
```bash
lsof -i :<port>   # find what is using the port
kill <pid>
```

**Docker build fails on libcurl**
Make sure you are building from inside the project root where the `Makefile` lives, not from a subdirectory.

**Coordinator does not start**
Check `coordinator_multi_group.log` (local) or `docker compose logs coordinator` (Docker) for the error.

**KV node refuses connections**
Each node writes its own log: `mg_node1.log` … `mg_node4.log`. Check the last 50 lines for the failure reason.

**Reset all data (local)**
```bash
rm -rf .ncloud_data/
```
