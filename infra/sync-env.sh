#!/usr/bin/env bash
#
# 로컬 .env.prod를 EC2의 /opt/waitforddaman/.env로 반영한다.
#
# 왜 스크립트인가: 파일 권한을 반드시 640(그룹 root 읽기)으로 맞춰야 하는데,
# Airflow 컨테이너가 uid 50000 / gid 0으로 돌면서 이 파일을
# /opt/airflow/.env로 마운트해 직접 source하기 때문이다.
# 600으로 두면 컨테이너가 못 읽어서 모든 DAG 태스크가 Permission denied로
# 죽는다(실제로 겪었음). 수동 scp로는 이 단계를 빼먹기 쉬워서 스크립트로 굳혔다.
#
# 사용법:
#   ./infra/sync-env.sh                  # 반영만
#   ./infra/sync-env.sh --restart        # 반영 후 컨테이너 재기동(환경변수 반영)
#
# 로컬 .env.prod가 source of truth다. 서버에서 직접 편집하지 말 것 - 편집하면
# 로컬과 갈려서 다음 sync에 덮어써진다. 서버 값을 가져오려면:
#   scp -i "$SSH_KEY" ec2-user@"$EC2_HOST":/opt/waitforddaman/.env ./.env.prod
set -euo pipefail

EC2_USER="${EC2_USER:-ec2-user}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/waitforddaman-prod-key.pem}"
REMOTE_DIR="/opt/waitforddaman"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env.prod"

# 서버 주소는 저장소에 하드코딩하지 않는다. gitignore되는 .env.prod에서 읽는다.
# `|| true`가 필요하다: 키가 없으면 grep이 1을 반환하고, set -e + pipefail 때문에
# 대입문에서 그대로 죽어버려 아래 안내 메시지에 도달하지 못한다.
if [[ -z "${EC2_HOST:-}" && -f "$ENV_FILE" ]]; then
  EC2_HOST="$(grep -m1 '^EC2_HOST=' "$ENV_FILE" | cut -d= -f2- || true)"
fi
if [[ -z "${EC2_HOST:-}" ]]; then
  echo "EC2_HOST를 찾을 수 없다. .env.prod에 EC2_HOST=<주소> 를 넣거나 환경변수로 지정하세요." >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "없음: $ENV_FILE" >&2
  echo ".env.example을 복사해서 실제 값을 채우거나, 서버에서 내려받으세요:" >&2
  echo "  scp -i $SSH_KEY $EC2_USER@$EC2_HOST:$REMOTE_DIR/.env $ENV_FILE" >&2
  exit 1
fi

# 플레이스홀더가 남아 있으면 배포 후 런타임에 터지므로 미리 잡는다.
# 주석이 아니라 실제 대입 라인만 본다 - 주석의 "<->" 같은 표기를 오탐하지 않게.
PLACEHOLDER_RE='^[A-Z_0-9]+=.*(<[a-zA-Z0-9 -]+>|CHANGE_ME)'
if grep -qE "$PLACEHOLDER_RE" "$ENV_FILE"; then
  echo "채우지 않은 플레이스홀더가 있습니다:" >&2
  grep -nE "$PLACEHOLDER_RE" "$ENV_FILE" | cut -d= -f1 >&2
  exit 1
fi

echo "==> $ENV_FILE -> $EC2_USER@$EC2_HOST:$REMOTE_DIR/.env"
scp -i "$SSH_KEY" "$ENV_FILE" "$EC2_USER@$EC2_HOST:$REMOTE_DIR/.env"

echo "==> 권한 설정 (컨테이너가 gid 0으로 읽을 수 있게)"
ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" \
  "sudo chown $EC2_USER:root $REMOTE_DIR/.env && chmod 640 $REMOTE_DIR/.env && ls -l $REMOTE_DIR/.env"

echo "==> 컨테이너에서 읽히는지 확인"
ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" \
  "docker exec airflow-scheduler head -1 /opt/airflow/.env >/dev/null && echo '읽기 OK' || echo '읽기 실패'"

if [[ "${1:-}" == "--restart" ]]; then
  echo "==> 컨테이너 재기동 (compose environment: 블록의 값 반영)"
  ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" \
    "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml up -d && docker compose -f docker-compose.prod.yml ps"
else
  echo ""
  echo "참고: DAG가 source하는 값은 다음 태스크 실행 때 바로 반영된다."
  echo "compose의 environment: 블록 값(AIRFLOW__*, DATABASE_URL 등)을 바꿨다면"
  echo "--restart 로 다시 실행해 컨테이너를 교체해야 한다."
fi
