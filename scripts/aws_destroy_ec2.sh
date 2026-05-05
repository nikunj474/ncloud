#!/usr/bin/env bash
set -euo pipefail

# Stop or terminate the EC2 instance created by scripts/aws_deploy_ec2.sh.
#
# Usage:
#   bash scripts/aws_destroy_ec2.sh stop
#   bash scripts/aws_destroy_ec2.sh terminate

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT_DIR/.aws-deploy/last-deploy.env"
ACTION="${1:-stop}"

if [ ! -f "$STATE_FILE" ]; then
  echo "[destroy] missing $STATE_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
. "$STATE_FILE"

case "$ACTION" in
  stop)
    echo "[destroy] stopping $INSTANCE_ID in $AWS_REGION"
    aws ec2 stop-instances --region "$AWS_REGION" --instance-ids "$INSTANCE_ID" >/dev/null
    ;;
  terminate)
    echo "[destroy] terminating $INSTANCE_ID in $AWS_REGION"
    aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids "$INSTANCE_ID" >/dev/null
    ;;
  *)
    echo "Usage: $0 [stop|terminate]" >&2
    exit 1
    ;;
esac

echo "[destroy] requested $ACTION"
