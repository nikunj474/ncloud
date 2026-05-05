#!/usr/bin/env bash
set -euo pipefail

# Deploy PennCloud to one AWS Academy EC2 instance.
#
# What this script does:
#   - Creates or reuses one EC2 instance named penncloud-demo.
#   - Creates or reuses an EC2 key pair and security group.
#   - Optionally allocates/reuses an Elastic IP.
#   - Uploads the current repository to /opt/penncloud on EC2.
#   - Runs the PennCloud Docker demo in multi-tablet mode.
#   - Starts nginx on the EC2 host as the public gateway for ports 80/443.
#   - Optionally configures Let's Encrypt HTTPS for a public DNS name.
#   - Optionally preserves KV data under /opt/penncloud-data/kv.
#   - Configures nginx with a high upload ceiling so Drive quota, not nginx,
#     decides whether a large upload is allowed.
#
# Prerequisites:
#   1. Start the AWS Academy Learner Lab and copy its temporary CLI credentials
#      into ~/.aws/credentials.
#   2. Make sure the AWS CLI works:
#        aws sts get-caller-identity
#   3. If using HTTPS, point your DNS name to the Elastic IP before running.
#      Example: liudawei.cis5550.net -> 100.22.215.193
#   4. If using external email relay, create a local .smtp.env first and run
#      with INCLUDE_SMTP_ENV=1. The file is uploaded to EC2 but not committed.
#
# Recommended full deployment:
#   INCLUDE_SMTP_ENV=1 AWS_REGION=us-west-2 \
#   DNS_NAME=liudawei.cis5550.net DOMAIN_NAME=liudawei.cis5550.net \
#   CERTBOT_EMAIL=your-email@example.com ENABLE_HTTPS=1 \
#   USE_ELASTIC_IP=1 PERSIST_DATA=1 \
#   bash scripts/aws_deploy_ec2.sh
#
# Minimal HTTP-only deployment:
#   AWS_REGION=us-west-2 bash scripts/aws_deploy_ec2.sh
#
# Reuse the previous admin token when redeploying:
#   set -a; . .aws-deploy/last-deploy.env; set +a
#   INCLUDE_SMTP_ENV=1 AWS_REGION=us-west-2 \
#   DNS_NAME=liudawei.cis5550.net DOMAIN_NAME=liudawei.cis5550.net \
#   CERTBOT_EMAIL=your-email@example.com ENABLE_HTTPS=1 \
#   USE_ELASTIC_IP=1 PERSIST_DATA=1 ADMIN_TOKEN="$ADMIN_TOKEN" \
#   bash scripts/aws_deploy_ec2.sh
#
# Main outputs:
#   - Main app:      http://<elastic-ip-or-public-ip>/
#   - DNS app:       http://<DNS_NAME>/
#   - HTTPS app:     https://<DOMAIN_NAME>/ when ENABLE_HTTPS=1
#   - Admin:         /admin?admin_token=<ADMIN_TOKEN>
#   - Direct FE URLs: ports 8090, 8091, 8092, mainly for debugging
#   - SMTP demo port: 2525
#   - Deployment state: .aws-deploy/last-deploy.env
#
# Optional environment variables:
#   AWS_ACCOUNT_ID      expected account id, default 214658736393
#   AWS_REGION          region, default from AWS CLI or us-east-1
#   INSTANCE_TYPE       default t3.medium
#   KEY_NAME            default penncloud-academy-key
#   SG_NAME             default penncloud-demo-sg
#   SSH_CIDR            default your current public IP /32
#   APP_CIDR            default 0.0.0.0/0 for HTTP/HTTPS/demo ports
#   INCLUDE_SMTP_ENV    set to 1 to upload local .smtp.env to EC2
#   ADMIN_TOKEN         admin token for /admin; random hex if omitted
#   DNS_NAME            optional DNS name / nginx server_name, e.g. liudawei.cis5550.net
#   USE_ELASTIC_IP      set to 1 to allocate/reuse and associate an Elastic IP
#   PERSIST_DATA        set to 1 to keep KV data under /opt/penncloud-data
#   ENABLE_HTTPS        set to 1 to request/configure Let's Encrypt HTTPS
#   DOMAIN_NAME         public DNS name for HTTPS; defaults to DNS_NAME
#   CERTBOT_EMAIL       email for Let's Encrypt registration when ENABLE_HTTPS=1
#   FORCE_HTTPS         set to 1 to redirect HTTP to HTTPS after cert setup
#
# Notes:
#   - AWS Academy credentials expire; update ~/.aws/credentials and rerun.
#   - Elastic IP keeps the public IP stable across EC2 restarts/redeployments.
#   - Persistent data only survives when PERSIST_DATA=1 and the same instance
#     or attached host path is reused.
#   - Let's Encrypt requires public port 80 to reach nginx for the challenge.
#   - nginx uses client_max_body_size 4096m so it is not the first bottleneck
#     for large Drive uploads; app-level quota still limits stored data.
#   - The script is idempotent for normal redeploys: it reuses EC2, key pair,
#     security group, Elastic IP, and existing certificates when possible.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/.aws-deploy"
mkdir -p "$STATE_DIR"

EXPECTED_ACCOUNT="${AWS_ACCOUNT_ID:-214658736393}"
REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"
REGION="${REGION:-us-east-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.medium}"
KEY_NAME="${KEY_NAME:-penncloud-academy-key}"
KEY_PATH="$STATE_DIR/${KEY_NAME}.pem"
SG_NAME="${SG_NAME:-penncloud-demo-sg}"
APP_CIDR="${APP_CIDR:-0.0.0.0/0}"
INCLUDE_SMTP_ENV="${INCLUDE_SMTP_ENV:-0}"
USE_ELASTIC_IP="${USE_ELASTIC_IP:-0}"
PERSIST_DATA="${PERSIST_DATA:-0}"
ENABLE_HTTPS="${ENABLE_HTTPS:-0}"
DOMAIN_NAME="${DOMAIN_NAME:-${DNS_NAME:-}}"
CERTBOT_EMAIL="${CERTBOT_EMAIL:-}"
FORCE_HTTPS="${FORCE_HTTPS:-0}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[deploy] missing required command: $1" >&2
    exit 1
  }
}

need aws
need ssh
need scp
need tar
need curl
need openssl

ADMIN_TOKEN="${ADMIN_TOKEN:-$(openssl rand -hex 16)}"

echo "[deploy] checking AWS identity..."
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
if [ -z "$ACCOUNT_ID" ]; then
  echo "[deploy] AWS CLI is not authenticated. Run: aws login" >&2
  exit 1
fi
if [ "$ACCOUNT_ID" != "$EXPECTED_ACCOUNT" ]; then
  echo "[deploy] wrong AWS account: got $ACCOUNT_ID, expected $EXPECTED_ACCOUNT" >&2
  exit 1
fi
if [ "$ENABLE_HTTPS" = "1" ]; then
  if [ -z "$DOMAIN_NAME" ]; then
    echo "[deploy] ENABLE_HTTPS=1 requires DOMAIN_NAME or DNS_NAME, e.g. liudawei.cis5550.net" >&2
    exit 1
  fi
  if [ -z "$CERTBOT_EMAIL" ]; then
    echo "[deploy] ENABLE_HTTPS=1 requires CERTBOT_EMAIL for Let's Encrypt registration" >&2
    exit 1
  fi
fi
echo "[deploy] using account $ACCOUNT_ID in region $REGION"

PUBLIC_IP="$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')"
SSH_CIDR="${SSH_CIDR:-${PUBLIC_IP}/32}"
echo "[deploy] SSH allowed from $SSH_CIDR"
echo "[deploy] app ports allowed from $APP_CIDR"

VPC_ID="$(aws ec2 describe-vpcs \
  --region "$REGION" \
  --filters Name=is-default,Values=true \
  --query 'Vpcs[0].VpcId' \
  --output text)"
if [ -z "$VPC_ID" ] || [ "$VPC_ID" = "None" ]; then
  echo "[deploy] no default VPC found in $REGION" >&2
  exit 1
fi

SUBNET_ID="$(aws ec2 describe-subnets \
  --region "$REGION" \
  --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
  --query 'Subnets[0].SubnetId' \
  --output text)"
if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" = "None" ]; then
  echo "[deploy] no default subnet found in $REGION / $VPC_ID" >&2
  exit 1
fi
echo "[deploy] VPC=$VPC_ID subnet=$SUBNET_ID"

if ! aws ec2 describe-key-pairs --region "$REGION" --key-names "$KEY_NAME" >/dev/null 2>&1; then
  echo "[deploy] creating key pair $KEY_NAME"
  aws ec2 create-key-pair \
    --region "$REGION" \
    --key-name "$KEY_NAME" \
    --query KeyMaterial \
    --output text > "$KEY_PATH"
  chmod 600 "$KEY_PATH"
else
  echo "[deploy] using existing key pair $KEY_NAME"
  if [ ! -f "$KEY_PATH" ]; then
    echo "[deploy] key exists in AWS but $KEY_PATH is missing. Put the private key there or set KEY_NAME." >&2
    exit 1
  fi
fi

SG_ID="$(aws ec2 describe-security-groups \
  --region "$REGION" \
  --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values="$SG_NAME" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || true)"
if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
  echo "[deploy] creating security group $SG_NAME"
  SG_ID="$(aws ec2 create-security-group \
    --region "$REGION" \
    --group-name "$SG_NAME" \
    --description "PennCloud browser demo" \
    --vpc-id "$VPC_ID" \
    --query GroupId \
    --output text)"
fi

authorize_ingress() {
  local proto="$1" port="$2" cidr="$3" desc="$4"
  aws ec2 authorize-security-group-ingress \
    --region "$REGION" \
    --group-id "$SG_ID" \
    --ip-permissions "IpProtocol=$proto,FromPort=$port,ToPort=$port,IpRanges=[{CidrIp=$cidr,Description='$desc'}]" \
    >/dev/null 2>&1 || true
}

authorize_ingress tcp 22 "$SSH_CIDR" "SSH"
authorize_ingress tcp 80 "$APP_CIDR" "PennCloud HTTP"
authorize_ingress tcp 443 "$APP_CIDR" "PennCloud HTTPS"
authorize_ingress tcp 8090 "$APP_CIDR" "PennCloud fe1"
authorize_ingress tcp 8091 "$APP_CIDR" "PennCloud fe2"
authorize_ingress tcp 8092 "$APP_CIDR" "PennCloud fe3"
authorize_ingress tcp 2525 "$APP_CIDR" "PennCloud SMTP demo"
authorize_ingress tcp 8088 "$APP_CIDR" "PennCloud frontend lb"
echo "[deploy] security group $SG_ID ready"

AMI_ID="$(aws ssm get-parameters \
  --region "$REGION" \
  --names /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query 'Parameters[0].Value' \
  --output text)"

EXISTING_ID="$(aws ec2 describe-instances \
  --region "$REGION" \
  --filters Name=tag:Name,Values=penncloud-demo Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' \
  --output text)"
if [ -n "$EXISTING_ID" ]; then
  INSTANCE_ID="$(echo "$EXISTING_ID" | awk '{print $1}')"
  STATE="$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].State.Name' --output text)"
  if [ "$STATE" = "stopped" ]; then
    echo "[deploy] starting existing instance $INSTANCE_ID"
    aws ec2 start-instances --region "$REGION" --instance-ids "$INSTANCE_ID" >/dev/null
  else
    echo "[deploy] reusing existing instance $INSTANCE_ID ($STATE)"
  fi
else
  echo "[deploy] launching EC2 $INSTANCE_TYPE from $AMI_ID"
  INSTANCE_ID="$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=penncloud-demo}]' \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --query 'Instances[0].InstanceId' \
    --output text)"
fi

echo "[deploy] waiting for instance $INSTANCE_ID..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"
aws ec2 wait instance-status-ok --region "$REGION" --instance-ids "$INSTANCE_ID"

ELASTIC_IP_ALLOCATION_ID=""
if [ "$USE_ELASTIC_IP" = "1" ]; then
  echo "[deploy] ensuring Elastic IP..."
  ELASTIC_IP_ALLOCATION_ID="$(aws ec2 describe-addresses \
    --region "$REGION" \
    --filters Name=tag:Name,Values=penncloud-demo-eip \
    --query 'Addresses[0].AllocationId' \
    --output text 2>/dev/null || true)"
  if [ -z "$ELASTIC_IP_ALLOCATION_ID" ] || [ "$ELASTIC_IP_ALLOCATION_ID" = "None" ]; then
    ELASTIC_IP_ALLOCATION_ID="$(aws ec2 allocate-address \
      --region "$REGION" \
      --domain vpc \
      --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=Name,Value=penncloud-demo-eip}]' \
      --query AllocationId \
      --output text)"
  fi
  ELASTIC_IP_ADDRESS="$(aws ec2 describe-addresses \
    --region "$REGION" \
    --allocation-ids "$ELASTIC_IP_ALLOCATION_ID" \
    --query 'Addresses[0].PublicIp' \
    --output text)"
  echo "[deploy] associating Elastic IP $ELASTIC_IP_ADDRESS with $INSTANCE_ID"
  aws ec2 associate-address \
    --region "$REGION" \
    --instance-id "$INSTANCE_ID" \
    --allocation-id "$ELASTIC_IP_ALLOCATION_ID" \
    --allow-reassociation >/dev/null
  sleep 5
fi

PUBLIC_DNS="$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicDnsName' \
  --output text)"
PUBLIC_IP_EC2="$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)"
PENNCLOUD_PUBLIC_HOST="${DOMAIN_NAME:-${DNS_NAME:-$PUBLIC_IP_EC2}}"
echo "[deploy] instance public DNS: $PUBLIC_DNS"

echo "[deploy] waiting for SSH..."
for _ in $(seq 1 60); do
  if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i "$KEY_PATH" "ec2-user@$PUBLIC_DNS" 'echo ok' >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

TMP_TAR="$STATE_DIR/penncloud-src.tar.gz"
echo "[deploy] packaging source..."
tar \
  --exclude='.git' \
  --exclude='.aws-deploy' \
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
scp -o StrictHostKeyChecking=no -i "$KEY_PATH" "$TMP_TAR" "ec2-user@$PUBLIC_DNS:/tmp/penncloud-src.tar.gz"
if [ "$INCLUDE_SMTP_ENV" = "1" ] && [ -f "$ROOT_DIR/.smtp.env" ]; then
  echo "[deploy] uploading .smtp.env because INCLUDE_SMTP_ENV=1"
  scp -o StrictHostKeyChecking=no -i "$KEY_PATH" "$ROOT_DIR/.smtp.env" "ec2-user@$PUBLIC_DNS:/tmp/penncloud.smtp.env"
fi

echo "[deploy] installing Docker, nginx, and starting PennCloud..."
ssh -o StrictHostKeyChecking=no -i "$KEY_PATH" "ec2-user@$PUBLIC_DNS" "INCLUDE_SMTP_ENV='$INCLUDE_SMTP_ENV' ADMIN_TOKEN='$ADMIN_TOKEN' PERSIST_DATA='$PERSIST_DATA' PUBLIC_IP_EC2='$PUBLIC_IP_EC2' PENNCLOUD_PUBLIC_HOST='$PENNCLOUD_PUBLIC_HOST' DNS_NAME='${DNS_NAME:-}' ENABLE_HTTPS='$ENABLE_HTTPS' DOMAIN_NAME='$DOMAIN_NAME' CERTBOT_EMAIL='$CERTBOT_EMAIL' FORCE_HTTPS='$FORCE_HTTPS' bash -s" <<'REMOTE'
set -euo pipefail
sudo dnf install -y docker nginx
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user || true
sudo rm -rf /opt/penncloud
sudo mkdir -p /opt/penncloud
if [ "${PERSIST_DATA:-0}" = "1" ]; then
  sudo mkdir -p /opt/penncloud-data/kv
fi
sudo tar -xzf /tmp/penncloud-src.tar.gz -C /opt/penncloud
if [ "${INCLUDE_SMTP_ENV:-0}" = "1" ] && [ -f /tmp/penncloud.smtp.env ]; then
  sudo cp /tmp/penncloud.smtp.env /opt/penncloud/.smtp.env
  sudo chmod 600 /opt/penncloud/.smtp.env
fi
sudo chown -R ec2-user:ec2-user /opt/penncloud
if [ "${PERSIST_DATA:-0}" = "1" ]; then
  sudo chown -R ec2-user:ec2-user /opt/penncloud-data
fi
sudo docker rm -f penncloud-demo >/dev/null 2>&1 || true
sudo docker run -d --name penncloud-demo \
  -e ADMIN_TOKEN="$ADMIN_TOKEN" \
  -e PENNCLOUD_PUBLIC_HOST="$PENNCLOUD_PUBLIC_HOST" \
  -p 8090:8090 -p 8091:8091 -p 8092:8092 -p 2525:2525 -p 8088:8088 \
  -v /opt/penncloud:/home/cis5050/workspace/sp26-cis5050-T05 \
  -v /opt/penncloud-data:/opt/penncloud-data \
  -w /home/cis5050/workspace/sp26-cis5050-T05 \
  cis5050/docker-env:gRPC sleep infinity
if [ "${PERSIST_DATA:-0}" = "1" ]; then
  sudo docker exec penncloud-demo bash -lc 'MODE=multi-tablet DATA_ROOT=/opt/penncloud-data/kv PRESERVE_DATA=1 bash start_browser_demo.sh'
else
  sudo docker exec penncloud-demo bash -lc 'MODE=multi-tablet bash start_browser_demo.sh'
fi
SERVER_NAME="${DNS_NAME:-_}"
if [ -n "${DOMAIN_NAME:-}" ]; then
  SERVER_NAME="$DOMAIN_NAME"
fi
CERTBOT_BIN="certbot"
install_certbot() {
  if command -v certbot >/dev/null 2>&1; then
    CERTBOT_BIN="$(command -v certbot)"
    return
  fi
  if sudo dnf install -y certbot >/dev/null 2>&1; then
    CERTBOT_BIN="$(command -v certbot)"
    return
  fi
  sudo dnf install -y python3 python3-pip >/dev/null
  sudo python3 -m venv /opt/certbot
  sudo /opt/certbot/bin/pip install --upgrade pip >/dev/null
  sudo /opt/certbot/bin/pip install certbot >/dev/null
  sudo ln -sf /opt/certbot/bin/certbot /usr/local/bin/certbot
  CERTBOT_BIN="/usr/local/bin/certbot"
}
write_nginx_http_only() {
sudo mkdir -p /etc/nginx/conf.d
sudo mkdir -p /var/www/certbot
sudo tee /etc/nginx/conf.d/penncloud.conf >/dev/null <<NGINX
upstream penncloud_frontends {
    server 127.0.0.1:8090 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8091 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8092 max_fails=1 fail_timeout=3s;
}

server {
    listen 80 default_server;
    server_name ${SERVER_NAME} ${PUBLIC_IP_EC2};
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

        # Direct FE redirect stubs use :8091/:8092. Through nginx, keep users
        # on the canonical entrypoint instead of exposing backend ports.
        proxy_redirect ~^http://([^/:]+):809[0-2](/.*)\$ \$scheme://\$host\$2;
    }
}
NGINX
}
write_nginx_https() {
sudo mkdir -p /etc/nginx/conf.d
sudo mkdir -p /var/www/certbot
local http_fallback='proxy_pass http://penncloud_frontends;'
if [ "${FORCE_HTTPS:-0}" = "1" ]; then
  http_fallback='return 301 https://$host$request_uri;'
fi
sudo tee /etc/nginx/conf.d/penncloud.conf >/dev/null <<NGINX
upstream penncloud_frontends {
    server 127.0.0.1:8090 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8091 max_fails=1 fail_timeout=3s;
    server 127.0.0.1:8092 max_fails=1 fail_timeout=3s;
}

server {
    listen 80 default_server;
    server_name ${SERVER_NAME} ${PUBLIC_IP_EC2};
    client_max_body_size 4096m;

    location ^~ /.well-known/acme-challenge/ {
        root /var/www/certbot;
        default_type "text/plain";
    }

    location / {
        ${http_fallback}
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
NGINX
}
write_nginx_http_only
sudo nginx -t
sudo systemctl enable nginx >/dev/null
sudo systemctl restart nginx
if [ "${ENABLE_HTTPS:-0}" = "1" ]; then
  install_certbot
  echo "[deploy] requesting/renewing Let's Encrypt certificate for ${DOMAIN_NAME}"
  sudo "$CERTBOT_BIN" certonly \
    --webroot -w /var/www/certbot \
    -d "$DOMAIN_NAME" \
    --email "$CERTBOT_EMAIL" \
    --agree-tos \
    --non-interactive \
    --keep-until-expiring
  write_nginx_https
  sudo nginx -t
  sudo systemctl restart nginx
  sudo tee /etc/systemd/system/penncloud-certbot-renew.service >/dev/null <<SYSTEMD_SERVICE
[Unit]
Description=Renew PennCloud Let's Encrypt certificates

[Service]
Type=oneshot
ExecStart=${CERTBOT_BIN} renew --quiet --deploy-hook "systemctl reload nginx"
SYSTEMD_SERVICE
  sudo tee /etc/systemd/system/penncloud-certbot-renew.timer >/dev/null <<SYSTEMD_TIMER
[Unit]
Description=Twice-daily PennCloud Let's Encrypt renewal check

[Timer]
OnCalendar=*-*-* 03,15:17:00
Persistent=true

[Install]
WantedBy=timers.target
SYSTEMD_TIMER
  sudo systemctl daemon-reload
  sudo systemctl enable --now penncloud-certbot-renew.timer >/dev/null
fi
REMOTE

echo "[deploy] checking public endpoint..."
for _ in $(seq 1 20); do
  if curl -fsS "http://$PUBLIC_IP_EC2/" >/dev/null &&
     curl -fsS "http://$PUBLIC_IP_EC2:8090/" >/dev/null; then
    break
  fi
  sleep 3
done
if [ "$ENABLE_HTTPS" = "1" ] && [ -n "$DOMAIN_NAME" ]; then
  for _ in $(seq 1 20); do
    if curl -fsS "https://$DOMAIN_NAME/" >/dev/null; then
      break
    fi
    sleep 3
  done
fi

cat > "$STATE_DIR/last-deploy.env" <<EOF_STATE
AWS_REGION=$REGION
INSTANCE_ID=$INSTANCE_ID
PUBLIC_DNS=$PUBLIC_DNS
PUBLIC_IP=$PUBLIC_IP_EC2
KEY_NAME=$KEY_NAME
KEY_PATH=$KEY_PATH
SECURITY_GROUP_ID=$SG_ID
ADMIN_TOKEN=$ADMIN_TOKEN
ELASTIC_IP_ALLOCATION_ID=$ELASTIC_IP_ALLOCATION_ID
USE_ELASTIC_IP=$USE_ELASTIC_IP
PERSIST_DATA=$PERSIST_DATA
ENABLE_HTTPS=$ENABLE_HTTPS
DOMAIN_NAME=$DOMAIN_NAME
PENNCLOUD_PUBLIC_HOST=$PENNCLOUD_PUBLIC_HOST
EOF_STATE

echo
echo "PennCloud is deployed."
echo "Main app:  http://$PUBLIC_IP_EC2/"
if [ -n "${DNS_NAME:-}" ]; then
  echo "DNS app:   http://$DNS_NAME/"
fi
if [ "$ENABLE_HTTPS" = "1" ] && [ -n "$DOMAIN_NAME" ]; then
  echo "HTTPS:     https://$DOMAIN_NAME/"
fi
echo "FE1 direct: http://$PUBLIC_IP_EC2:8090/"
echo "Admin:     http://$PUBLIC_IP_EC2/admin?admin_token=$ADMIN_TOKEN"
if [ -n "${DNS_NAME:-}" ]; then
  echo "DNS admin: http://$DNS_NAME/admin?admin_token=$ADMIN_TOKEN"
fi
if [ "$ENABLE_HTTPS" = "1" ] && [ -n "$DOMAIN_NAME" ]; then
  echo "HTTPS admin: https://$DOMAIN_NAME/admin?admin_token=$ADMIN_TOKEN"
fi
echo "FE2 admin: http://$PUBLIC_IP_EC2:8091/admin?admin_token=$ADMIN_TOKEN"
echo "FE3 admin: http://$PUBLIC_IP_EC2:8092/admin?admin_token=$ADMIN_TOKEN"
echo "SMTP:      $PUBLIC_IP_EC2:2525"
if [ -n "$ELASTIC_IP_ALLOCATION_ID" ]; then
  echo "Elastic IP allocation: $ELASTIC_IP_ALLOCATION_ID"
fi
if [ "$PERSIST_DATA" = "1" ]; then
  echo "Persistent data: /opt/penncloud-data/kv"
fi
echo
echo "State saved to $STATE_DIR/last-deploy.env"
