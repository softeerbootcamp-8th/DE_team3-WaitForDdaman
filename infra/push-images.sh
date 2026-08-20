#!/usr/bin/env bash
#
# 프로덕션 이미지 3개를 빌드해서 ECR에 push한다.
#
# GitHub Actions에서 ECR로 직접 push하려면 OIDC IAM 역할이 필요한데 계정 권한이
# 없어서(SCP가 MFA 없는 요청을 차단) 이 단계만 로컬에서 수동으로 돌린다.
# 배포 자체(EC2에서 pull + up -d)는 .github/workflows/cd.yml이 자동으로 처리한다.
#
# 사전 준비 (하루 한 번 정도, refresh token 만료되면):
#   aws login --profile console
#
# 사용법:
#   ./infra/push-images.sh              # :latest 태그로 push
#   ./infra/push-images.sh <태그>        # 지정 태그 + :latest 둘 다 push
#
set -euo pipefail

AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-console}"
AWS_REGION="${AWS_REGION:-ap-northeast-2}"
# 레지스트리 주소(=AWS 계정 ID 포함)는 저장소에 하드코딩하지 않는다.
# gitignore되는 .env.prod에서 읽고, 없으면 환경변수로 받는다.
# `|| true`가 필요하다: 키가 없으면 grep이 1을 반환하고, set -e + pipefail 때문에
# 대입문에서 그대로 죽어버려 아래 안내 메시지에 도달하지 못한다.
if [[ -z "${ECR_REGISTRY:-}" && -f "$(dirname "${BASH_SOURCE[0]}")/../.env.prod" ]]; then
  ECR_REGISTRY="$(grep -m1 '^ECR_REGISTRY=' "$(dirname "${BASH_SOURCE[0]}")/../.env.prod" | cut -d= -f2- || true)"
fi
if [[ -z "${ECR_REGISTRY:-}" ]]; then
  echo "ECR_REGISTRY를 찾을 수 없다. .env.prod에 넣거나 환경변수로 지정하세요:" >&2
  echo "  ECR_REGISTRY=<account-id>.dkr.ecr.ap-northeast-2.amazonaws.com $0" >&2
  exit 1
fi
REGISTRY="$ECR_REGISTRY"
EXTRA_TAG="${1:-}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> ECR 로그인 ($REGISTRY, profile=$AWS_PROFILE_NAME)"
if ! aws ecr get-login-password --profile "$AWS_PROFILE_NAME" --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "$REGISTRY"; then
  echo "" >&2
  echo "ECR 로그인 실패. 자격증명이 만료됐을 수 있다:" >&2
  echo "  aws login --profile $AWS_PROFILE_NAME" >&2
  exit 1
fi

# name:dockerfile:context
IMAGES=(
  "waitforddaman-airflow:airflow/Dockerfile.prod:."
  "waitforddaman-api:services/api/Dockerfile.prod:."
  "waitforddaman-web:services/web/Dockerfile.prod:services/web"
)

for spec in "${IMAGES[@]}"; do
  IFS=':' read -r name dockerfile context <<< "$spec"

  tag_args=(-t "${REGISTRY}/${name}:latest")
  if [[ -n "$EXTRA_TAG" ]]; then
    tag_args+=(-t "${REGISTRY}/${name}:${EXTRA_TAG}")
  fi

  echo ""
  echo "==> 빌드: $name ($dockerfile)"
  # EC2가 t4g.large(arm64)라 arm64 이미지가 필요하다. 맥(Apple Silicon)에서는
  # 네이티브라 --platform 없이도 되지만, x86 머신에서 돌릴 때도 맞게 나오도록 명시한다.
  docker build --platform linux/arm64 -f "$dockerfile" "${tag_args[@]}" "$context"

  echo "==> push: $name"
  docker push "${REGISTRY}/${name}:latest"
  if [[ -n "$EXTRA_TAG" ]]; then
    docker push "${REGISTRY}/${name}:${EXTRA_TAG}"
  fi
done

echo ""
echo "완료. 이미지 3개가 ECR에 올라갔다."
echo "배포는 production 브랜치에 push하면 cd.yml이 자동으로 처리한다."
echo "지금 바로 수동 배포하려면:"
echo "  ssh -i ~/.ssh/waitforddaman-prod-key.pem ec2-user@<EC2_HOST> \\"
echo "    'cd /opt/waitforddaman && docker compose -f docker-compose.prod.yml pull && docker compose -f docker-compose.prod.yml up -d'"
