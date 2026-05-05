#!/usr/bin/env bash
set -euo pipefail

# Deploy PennCloud to one AWS Academy EC2 instance.
#
# Usage:
#   AWS_REGION=us-east-1 bash scripts/aws_deploy_ec2.sh
#
# Optional environment variables:
#   AWS_ACCOUNT_ID      expected account id, default 214658736393
#   AWS_REGION          region, default from AWS CLI or us-east-1
#   INSTANCE_TYPE       default t3.medium
#   KEY_NAME            default penncloud-academy-key
#   SSH_CIDR            default your current public IP /32
#   APP_CIDR            default 0.0.0.0/0 for browser demo ports
#   INCLUDE_SMTP_ENV    set to 1 to upload local .smtp.env to EC2
#   ADMIN_TOKEN         admin token for /admin; random hex if omitted
#   DNS_NAME            optional DNS name to print, e.g. liudawei.cis5550.net

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

echo "[deploy] installing Docker and starting PennCloud..."
ssh -o StrictHostKeyChecking=no -i "$KEY_PATH" "ec2-user@$PUBLIC_DNS" "INCLUDE_SMTP_ENV='$INCLUDE_SMTP_ENV' ADMIN_TOKEN='$ADMIN_TOKEN' bash -s" <<'REMOTE'
set -euo pipefail
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user || true
sudo rm -rf /opt/penncloud
sudo mkdir -p /opt/penncloud
sudo tar -xzf /tmp/penncloud-src.tar.gz -C /opt/penncloud
if [ "${INCLUDE_SMTP_ENV:-0}" = "1" ] && [ -f /tmp/penncloud.smtp.env ]; then
  sudo cp /tmp/penncloud.smtp.env /opt/penncloud/.smtp.env
  sudo chmod 600 /opt/penncloud/.smtp.env
fi
sudo chown -R ec2-user:ec2-user /opt/penncloud
sudo docker rm -f penncloud-demo >/dev/null 2>&1 || true
sudo docker run -d --name penncloud-demo \
  -e ADMIN_TOKEN="$ADMIN_TOKEN" \
  -p 80:8090 -p 8090:8090 -p 8091:8091 -p 8092:8092 -p 2525:2525 -p 8088:8088 \
  -v /opt/penncloud:/home/cis5050/workspace/sp26-cis5050-T05 \
  -w /home/cis5050/workspace/sp26-cis5050-T05 \
  cis5050/docker-env:gRPC sleep infinity
sudo docker exec penncloud-demo bash -lc 'MODE=multi-tablet bash start_browser_demo.sh'
REMOTE

echo "[deploy] checking public endpoint..."
for _ in $(seq 1 20); do
  if curl -fsS "http://$PUBLIC_IP_EC2/" >/dev/null &&
     curl -fsS "http://$PUBLIC_IP_EC2:8090/" >/dev/null; then
    break
  fi
  sleep 3
done

cat > "$STATE_DIR/last-deploy.env" <<EOF_STATE
AWS_REGION=$REGION
INSTANCE_ID=$INSTANCE_ID
PUBLIC_DNS=$PUBLIC_DNS
PUBLIC_IP=$PUBLIC_IP_EC2
KEY_NAME=$KEY_NAME
KEY_PATH=$KEY_PATH
SECURITY_GROUP_ID=$SG_ID
ADMIN_TOKEN=$ADMIN_TOKEN
EOF_STATE

echo
echo "PennCloud is deployed."
echo "Main app:  http://$PUBLIC_IP_EC2/"
if [ -n "${DNS_NAME:-}" ]; then
  echo "DNS app:   http://$DNS_NAME/"
fi
echo "FE1 direct: http://$PUBLIC_IP_EC2:8090/"
echo "Admin:     http://$PUBLIC_IP_EC2/admin?admin_token=$ADMIN_TOKEN"
if [ -n "${DNS_NAME:-}" ]; then
  echo "DNS admin: http://$DNS_NAME/admin?admin_token=$ADMIN_TOKEN"
fi
echo "FE2 admin: http://$PUBLIC_IP_EC2:8091/admin?admin_token=$ADMIN_TOKEN"
echo "FE3 admin: http://$PUBLIC_IP_EC2:8092/admin?admin_token=$ADMIN_TOKEN"
echo "SMTP:      $PUBLIC_IP_EC2:2525"
echo
echo "State saved to $STATE_DIR/last-deploy.env"
