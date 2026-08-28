#!/usr/bin/env bash
set -euo pipefail

# Deploy PennCloud to an Oracle Cloud (or any Ubuntu) VM.
#
# What this script does:
#   - SSHes into the target VM via ~/.ssh/oracle_arm (or SSH_KEY).
#   - Installs Docker and nginx via apt if missing.
#   - Sets up a 4 GB swap file when one does not exist.
#   - Uploads the current repository, builds it inside the Docker image.
#   - Starts PennCloud in multi-tablet mode (3 KV nodes + coordinator + 3 FE).
#   - Configures nginx for the public domain (HTTP + HTTPS).
#   - Obtains/renews a Let's Encrypt certificate and forces HTTPS.
#   - Sets up a systemd timer for automatic certificate renewal.
#
# Recommended usage:
#   INCLUDE_SMTP_ENV=1 \
#   DOMAIN_NAME=penncloud.example.com \
#   CERTBOT_EMAIL=you@example.com \
#   PERSIST_DATA=1 \
#   bash scripts/oracle_deploy.sh
#
# Environment variables:
#   SSH_HOST          hostname/IP of the VM, default 146.235.197.133
#   SSH_USER          SSH user, default ubuntu
#   SSH_KEY           SSH private key, default ~/.ssh/oracle_arm
#   DOMAIN_NAME       public DNS name, default penncloud.example.com
#   CERTBOT_EMAIL     email for Let's Encrypt
#   INCLUDE_SMTP_ENV  set to 1 to upload local .smtp.env
#   PERSIST_DATA      set to 1 to keep KV data under /opt/penncloud-data
#   ADMIN_TOKEN       admin panel token; random hex if omitted
#   FORCE_HTTPS       set to 0 to serve HTTP alongside HTTPS (default 1)
#
# PennCloud uses these ports (all are on localhost/127.0.0.1 except nginx):
#   80/443  nginx (public)
#   8090-8092  Frontend servers (loopback only, proxied by nginx)
#   2525    SMTP demo
#   8088    Frontend load-balancer (not used in multi-tablet mode)
#
# The other.example.com services are expected to use a separate nginx
# server_name block or a different port range; we write our config only to
# /etc/nginx/conf.d/penncloud.conf and do not touch other vhosts.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/.oracle-deploy"
mkdir -p "$STATE_DIR"

SSH_HOST="${SSH_HOST:-146.235.197.133}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/oracle_arm}"
DOMAIN_NAME="${DOMAIN_NAME:-penncloud.example.com}"
CERTBOT_EMAIL="${CERTBOT_EMAIL:-you@example.com}"
INCLUDE_SMTP_ENV="${INCLUDE_SMTP_ENV:-0}"
PERSIST_DATA="${PERSIST_DATA:-1}"
FORCE_HTTPS="${FORCE_HTTPS:-1}"
ADMIN_TOKEN="${ADMIN_TOKEN:-$(openssl rand -hex 16)}"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -i $SSH_KEY"
REMOTE="${SSH_USER}@${SSH_HOST}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "[deploy] missing: $1" >&2; exit 1; }; }
need ssh
need scp
need tar
need openssl

echo "[deploy] target: $REMOTE  domain: $DOMAIN_NAME"

# ── 1. Package source ────────────────────────────────────────────────────────
TMP_TAR="$STATE_DIR/penncloud-src.tar.gz"
echo "[deploy] packaging source..."
tar \
  --exclude='.git' \
  --exclude='.aws-deploy' \
  --exclude='.oracle-deploy' \
  --exclude='*.log' \
  --exclude='*.pid' \
  --exclude='*/build' \
  --exclude='kvstore/kvserver' \
  --exclude='frontend/feserver' \
  --exclude='coordinator/coordinator' \
  --exclude='smtp_server/smtp_server' \
  --exclude='frontend_lb/frontend_lb' \
  --exclude='.env' \
  --exclude='.env.*' \
  --exclude='*.env' \
  --exclude='.smtp.env' \
  -czf "$TMP_TAR" -C "$ROOT_DIR" .

echo "[deploy] uploading source..."
scp $SSH_OPTS "$TMP_TAR" "$REMOTE:/tmp/penncloud-src.tar.gz"

if [ "${INCLUDE_SMTP_ENV:-0}" = "1" ] && [ -f "$ROOT_DIR/.smtp.env" ]; then
  echo "[deploy] uploading .smtp.env..."
  scp $SSH_OPTS "$ROOT_DIR/.smtp.env" "$REMOTE:/tmp/penncloud.smtp.env"
fi

# ── 2. Remote provisioning ───────────────────────────────────────────────────
echo "[deploy] provisioning remote VM..."
ssh $SSH_OPTS "$REMOTE" \
  "INCLUDE_SMTP_ENV='$INCLUDE_SMTP_ENV' \
   ADMIN_TOKEN='$ADMIN_TOKEN' \
   PERSIST_DATA='$PERSIST_DATA' \
   DOMAIN_NAME='$DOMAIN_NAME' \
   CERTBOT_EMAIL='$CERTBOT_EMAIL' \
   FORCE_HTTPS='$FORCE_HTTPS' \
   bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# ── fix apt sources: use HTTPS (Oracle Cloud blocks outbound port 80) ────────
echo "[remote] switching apt sources to HTTPS..."
if grep -qr 'http://ports.ubuntu.com' /etc/apt/sources.list /etc/apt/sources.list.d/ 2>/dev/null; then
  sudo sed -i 's|http://ports.ubuntu.com|https://ports.ubuntu.com|g' /etc/apt/sources.list
  for f in /etc/apt/sources.list.d/*.list; do
    [ -f "$f" ] && sudo sed -i 's|http://ports.ubuntu.com|https://ports.ubuntu.com|g' "$f" || true
  done
fi

# ── apt packages ────────────────────────────────────────────────────────────
echo "[remote] installing packages..."
sudo apt-get update -qq 2>&1 | grep -v '^Hit:\|^Get:\|^Ign:' || true
sudo apt-get install -y -qq \
  docker.io nginx certbot python3-certbot-nginx \
  curl ca-certificates openssl 2>&1 | grep -v '^Get:\|^Hit:\|^Ign:\|^Preparing\|^Unpacking\|^Setting up\|^Processing' || true
sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu || true

# ── swap ────────────────────────────────────────────────────────────────────
SWAP_BYTES="$(stat -c%s /swapfile 2>/dev/null || echo 0)"
if [ "$SWAP_BYTES" -lt 4294967296 ]; then
  echo "[remote] creating 4 GB swap..."
  sudo swapoff /swapfile 2>/dev/null || true
  sudo rm -f /swapfile
  sudo fallocate -l 4G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
fi
sudo swapon /swapfile 2>/dev/null || true
if ! grep -q '^/swapfile ' /etc/fstab; then
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

# ── unpack source ────────────────────────────────────────────────────────────
echo "[remote] unpacking source..."
sudo rm -rf /opt/penncloud
sudo mkdir -p /opt/penncloud
if [ "${PERSIST_DATA:-0}" = "1" ]; then
  sudo mkdir -p /opt/penncloud-data/kv
fi
sudo tar -xzf /tmp/penncloud-src.tar.gz -C /opt/penncloud
if [ "${INCLUDE_SMTP_ENV:-0}" = "1" ] && [ -f /tmp/penncloud.smtp.env ]; then
  sudo cp /tmp/penncloud.smtp.env /opt/penncloud/.smtp.env
  sudo chmod 644 /opt/penncloud/.smtp.env
fi
# Use UID 1000 explicitly: the Docker cis5050 user is UID 1000,
# while the Oracle ubuntu user may be UID 1001.
sudo chown -R 1000:1000 /opt/penncloud
if [ "${PERSIST_DATA:-0}" = "1" ]; then
  sudo chown -R 1000:1000 /opt/penncloud-data
fi

# ── Docker container ─────────────────────────────────────────────────────────
echo "[remote] starting Docker container..."
sudo docker rm -f penncloud-oracle 2>/dev/null || true
sudo docker run -d --name penncloud-oracle \
  -e ADMIN_TOKEN="$ADMIN_TOKEN" \
  -e PENNCLOUD_PUBLIC_HOST="$DOMAIN_NAME" \
  --restart unless-stopped \
  -p 127.0.0.1:8090:8090 \
  -p 127.0.0.1:8091:8091 \
  -p 127.0.0.1:8092:8092 \
  -p 0.0.0.0:2525:2525 \
  -v /opt/penncloud:/home/cis5050/workspace/sp26-cis5050-T05 \
  -v /opt/penncloud-data:/opt/penncloud-data \
  -w /home/cis5050/workspace/sp26-cis5050-T05 \
  cis5050/docker-env:gRPC sleep infinity

if [ "${PERSIST_DATA:-0}" = "1" ]; then
  sudo docker exec penncloud-oracle bash -lc \
    'MODE=multi-tablet DATA_ROOT=/opt/penncloud-data/kv PRESERVE_DATA=1 bash start_browser_demo.sh'
else
  sudo docker exec penncloud-oracle bash -lc \
    'MODE=multi-tablet bash start_browser_demo.sh'
fi

# ── nginx: http-only first (needed for ACME challenge) ───────────────────────
echo "[remote] writing nginx HTTP config for ACME..."
sudo mkdir -p /etc/nginx/conf.d /var/www/certbot

sudo tee /etc/nginx/conf.d/penncloud.conf >/dev/null <<NGINX_HTTP
upstream penncloud_frontends {
    server 127.0.0.1:8090 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8091 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8092 max_fails=1 fail_timeout=3s;
}

server {
    listen 80;
    server_name ${DOMAIN_NAME};
    client_max_body_size 4096m;

    location ^~ /.well-known/acme-challenge/ {
        root /var/www/certbot;
        default_type "text/plain";
    }

    location / {
        proxy_pass http://penncloud_frontends;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Host \$host;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Connection "";
        proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
        proxy_next_upstream_tries 3;
        proxy_connect_timeout 1s;
        proxy_request_buffering off;
        proxy_read_timeout 900s;
        proxy_send_timeout 900s;
        proxy_redirect ~^http://([^/:]+):809[0-2](/.*)\$ \$scheme://\$host\$2;
    }
}
NGINX_HTTP

sudo nginx -t
sudo systemctl enable nginx >/dev/null
sudo systemctl restart nginx

# ── Let's Encrypt cert ────────────────────────────────────────────────────────
echo "[remote] obtaining Let's Encrypt certificate for ${DOMAIN_NAME}..."
sudo certbot certonly \
  --webroot -w /var/www/certbot \
  -d "$DOMAIN_NAME" \
  --email "$CERTBOT_EMAIL" \
  --agree-tos \
  --non-interactive \
  --keep-until-expiring

# ── nginx: full HTTPS config ──────────────────────────────────────────────────
echo "[remote] writing nginx HTTPS config..."
HTTP_LOCATION='return 301 https://$host$request_uri;'
if [ "${FORCE_HTTPS:-1}" != "1" ]; then
  HTTP_LOCATION='proxy_pass http://penncloud_frontends;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";
        proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
        proxy_next_upstream_tries 3;
        proxy_connect_timeout 1s;
        proxy_request_buffering off;
        proxy_read_timeout 900s;
        proxy_send_timeout 900s;
        proxy_redirect ~^http://([^/:]+):809[0-2](/.*)$ $scheme://$host$2;'
fi

sudo tee /etc/nginx/conf.d/penncloud.conf >/dev/null <<NGINX_HTTPS
upstream penncloud_frontends {
    server 127.0.0.1:8090 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8091 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8092 max_fails=1 fail_timeout=3s;
}

server {
    listen 80;
    server_name ${DOMAIN_NAME};

    location ^~ /.well-known/acme-challenge/ {
        root /var/www/certbot;
        default_type "text/plain";
    }

    location / {
        return 301 https://\$host\$request_uri;
    }
}

server {
    listen 443 ssl;
    server_name ${DOMAIN_NAME};
    client_max_body_size 4096m;

    ssl_certificate /etc/letsencrypt/live/${DOMAIN_NAME}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${DOMAIN_NAME}/privkey.pem;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers off;
    add_header Strict-Transport-Security "max-age=63072000" always;

    location / {
        proxy_pass http://penncloud_frontends;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Host \$host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header Connection "";
        proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
        proxy_next_upstream_tries 3;
        proxy_connect_timeout 1s;
        proxy_request_buffering off;
        proxy_read_timeout 900s;
        proxy_send_timeout 900s;
        proxy_redirect ~^http://([^/:]+):809[0-2](/.*)\$ https://\$host\$2;
    }
}
NGINX_HTTPS

sudo nginx -t
sudo systemctl reload nginx

# ── certbot auto-renewal (systemd timer) ─────────────────────────────────────
echo "[remote] setting up certbot auto-renewal..."
sudo tee /etc/systemd/system/penncloud-certbot-renew.service >/dev/null <<SVCUNIT
[Unit]
Description=Renew PennCloud Let's Encrypt certificates

[Service]
Type=oneshot
ExecStart=/usr/bin/certbot renew --quiet --deploy-hook "systemctl reload nginx"
SVCUNIT

sudo tee /etc/systemd/system/penncloud-certbot-renew.timer >/dev/null <<TIMERUNIT
[Unit]
Description=Twice-daily PennCloud Let's Encrypt renewal check

[Timer]
OnCalendar=*-*-* 03,15:17:00
Persistent=true

[Install]
WantedBy=timers.target
TIMERUNIT

sudo systemctl daemon-reload
sudo systemctl enable --now penncloud-certbot-renew.timer >/dev/null

echo "[remote] done."
REMOTE_SCRIPT

# ── 3. Health check ───────────────────────────────────────────────────────────
echo "[deploy] waiting for service to be reachable..."
for _ in $(seq 1 30); do
  if curl -fsS --max-time 5 "https://$DOMAIN_NAME/" >/dev/null 2>&1; then
    break
  fi
  sleep 3
done

# ── 4. Save state ─────────────────────────────────────────────────────────────
cat > "$STATE_DIR/last-deploy.env" <<EOF_STATE
SSH_HOST=$SSH_HOST
SSH_USER=$SSH_USER
SSH_KEY=$SSH_KEY
DOMAIN_NAME=$DOMAIN_NAME
ADMIN_TOKEN=$ADMIN_TOKEN
PERSIST_DATA=$PERSIST_DATA
FORCE_HTTPS=$FORCE_HTTPS
EOF_STATE

echo
echo "================================================================"
echo "  PennCloud deployed on Oracle VM"
echo "================================================================"
echo "  Main app:    https://$DOMAIN_NAME/"
echo "  HTTP->HTTPS: http://$DOMAIN_NAME/  (redirects)"
echo "  Admin:       https://$DOMAIN_NAME/admin?admin_token=$ADMIN_TOKEN"
echo "  FE1 (local): http://127.0.0.1:8090/  (loopback only)"
echo "  SMTP demo:   $SSH_HOST:2525"
echo "  State saved to $STATE_DIR/last-deploy.env"
echo "================================================================"
echo
echo "Rerun with same token:"
echo "  ADMIN_TOKEN=$ADMIN_TOKEN INCLUDE_SMTP_ENV=1 \\"
echo "  DOMAIN_NAME=$DOMAIN_NAME CERTBOT_EMAIL=$CERTBOT_EMAIL \\"
echo "  PERSIST_DATA=1 bash scripts/oracle_deploy.sh"
