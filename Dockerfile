# ── build stage ───────────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ make pkg-config libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN make -C coordinator \
 && make -C kvstore \
 && make -C frontend \
 && make -C smtp_server \
 && make -C frontend_lb

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        libcurl4 netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /src/coordinator/coordinator  ./coordinator
COPY --from=builder /src/kvstore/kvserver         ./kvserver
COPY --from=builder /src/frontend/feserver        ./feserver
COPY --from=builder /src/smtp_server/smtp_server  ./smtp_server
COPY --from=builder /src/frontend_lb/frontend_lb  ./frontend_lb
