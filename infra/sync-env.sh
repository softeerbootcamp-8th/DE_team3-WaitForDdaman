#!/usr/bin/env bash
#
# 로컬 .env.prod를 EC2의 /opt/waitforddaman/.env로 반영한다.
# EC2 서버 분리(Services, Airflow)를 지원합니다.
#
# 왜 스크립트인가: 파일 권한을 반드시 640(그룹 root 읽기)으로 맞춰야 하는데,
# Airflow 컨테이너가 uid 50000 / gid 0으로 돌면서 이 파일을
# /opt/airflow/ingestion/.env로 마운트해 직접 source하기 때문이다.
# 600으로 두면 컨테이너가 못 읽어서 모든 DAG 태스크가 Permission denied로
# 죽는다(실제로 겪었음). 수동 scp로는 이 단계를 빼먹기 쉬워서 스크립트로 굳혔다.
#
# 사용법:
#   ./infra/sync-env.sh                  # 두 서버(Airflow, Services) 모두 반영
#   ./infra/sync-env.sh --airflow        # Airflow 서버에만 반영
#   ./infra/sync-env.sh --services       # Services(Web/API) 서버에만 반영
#   ./infra/sync-env.sh --restart        # 반영 후 각 서버의 compose 컨테이너 재기동
#
# 로컬 .env.prod가 source of truth다. 서버에서 직접 편집하지 말 것.
set -euo pipefail

EC2_USER="${EC2_USER:-ec2-user}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/waitforddaman-prod-key.pem}"
REMOTE_DIR="/opt/waitforddaman"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env.prod"

TARGET="all"
DO_RESTART=false

for arg in "$@"; do
  case "$arg" in
    --airflow) TARGET="airflow" ;;
    --services) TARGET="services" ;;
    --restart) DO_RESTART=true ;;
    *) echo "알 수 없는 옵션: $arg (사용법: $0 [--airflow|--services] [--restart])" >&2; exit 1 ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "없음: $ENV_FILE" >&2
  echo ".env.example을 복사해서 실제 값을 채우세요." >&2
  exit 1
fi

# 호스트 주소 읽기 (환경변수 우선, 없으면 .env.prod에서 검색)
AIRFLOW_HOST="${AIRFLOW_EC2_HOST:-$(grep -m1 '^AIRFLOW_EC2_HOST=' "$ENV_FILE" | cut -d= -f2- || grep -m1 '^EC2_HOST=' "$ENV_FILE" | cut -d= -f2- || true)}"
SERVICES_HOST="${SERVICES_EC2_HOST:-$(grep -m1 '^SERVICES_EC2_HOST=' "$ENV_FILE" | cut -d= -f2- || true)}"

# 플레이스홀더 검사
PLACEHOLDER_RE='^[A-Z_0-9]+=.*(<[a-zA-Z0-9 -]+>|CHANGE_ME)'
if grep -qE "$PLACEHOLDER_RE" "$ENV_FILE"; then
  echo "채우지 않은 플레이스홀더가 있습니다:" >&2
  grep -nE "$PLACEHOLDER_RE" "$ENV_FILE" | cut -d= -f1 >&2
  exit 1
fi

sync_to_host() {
  local host="$1"
  local compose_file="$2"
  local label="$3"

  if [[ -z "$host" ]]; then
    echo "[$label] 호스트 IP가 지정되지 않아 스킵합니다."
    return 0
  fi

  echo "=================================================="
  echo "==> [$label] $ENV_FILE -> $EC2_USER@$host:$REMOTE_DIR/.env"
  echo "=================================================="

  scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$SSH_KEY" "$ENV_FILE" "$EC2_USER@$host:$REMOTE_DIR/.env"

  echo "==> [$label] 권한 설정 (컨테이너가 gid 0으로 읽을 수 있게 640 설정)"
  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$SSH_KEY" "$EC2_USER@$host" \
    "sudo chown $EC2_USER:root $REMOTE_DIR/.env && chmod 640 $REMOTE_DIR/.env && ls -l $REMOTE_DIR/.env"

  if [[ "$DO_RESTART" == true ]]; then
    echo "==> [$label] 컨테이너 재기동 ($compose_file)"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$SSH_KEY" "$EC2_USER@$host" \
      "cd $REMOTE_DIR && (docker compose -f $compose_file up -d || docker compose -f docker-compose.prod.yml up -d) && (docker compose -f $compose_file ps || docker compose ps)"
  fi
}

if [[ "$TARGET" == "all" || "$TARGET" == "airflow" ]]; then
  sync_to_host "$AIRFLOW_HOST" "docker-compose.airflow.prod.yml" "Airflow"
fi

if [[ "$TARGET" == "all" || "$TARGET" == "services" ]]; then
  sync_to_host "$SERVICES_HOST" "docker-compose.services.prod.yml" "Services"
fi

echo ""
echo "동기화 완료."
