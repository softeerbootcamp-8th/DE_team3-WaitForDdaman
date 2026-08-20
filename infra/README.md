# infra

프로덕션(EC2 + ECR + docker compose + GitHub Actions) 배포 설정과 절차.

## 아키텍처 요약

- 단일 EC2 인스턴스(`t4g.large`, arm64/Graviton2) 위에서 `docker compose`로 전체 스택 실행:
  Postgres(Airflow 메타DB + bikeman/serving 스키마), Airflow 4개 컨테이너
  (apiserver/scheduler/dag-processor/triggerer) + airflow-init, `services/api`(FastAPI),
  `services/web`(nginx + React 정적 빌드).
- ingestion/staging PySpark 잡은 별도 Spark 클러스터 없이 Airflow 컨테이너 안에서
  직접 실행된다(JVM 포함 이미지, `airflow/Dockerfile.prod`). **추후 EMR 도입 예정** —
  전환되면 이 부분 아키텍처가 바뀔 수 있음.
- 이미지는 ECR 3개 리포지토리(`waitforddaman-airflow`, `-api`, `-web`)에 저장, EC2는
  인스턴스 프로파일 IAM Role로 pull(정적 AWS 키 미사용).
- Iceberg 카탈로그는 `hadoop`로 고정(Glue 권한 없음).
- serving 스키마는 추후 RDS(`db.t4g.medium`)로 이전 예정(다른 담당자 영역, 이번 범위 아님).

## 사전 준비 (AWS 콘솔, 1회)

1. **ECR**: `ap-northeast-2`에 리포지토리 3개 생성 — `waitforddaman-airflow`, `waitforddaman-api`, `waitforddaman-web`.
2. **IAM 역할(EC2용)**: `waitforddaman-ec2-role` — trusted entity: EC2.
   - 커스텀 정책(`waitforddaman-s3-access`): `RAW_BUCKET`/`WAREHOUSE_BUCKET`에 대해
     `s3:GetObject/PutObject/DeleteObject/ListBucket`.
   - AWS 관리형 `AmazonEC2ContainerRegistryReadOnly` (ECR pull).
   - Glue 권한은 부여하지 않음(hadoop 카탈로그라 불필요).
3. **보안그룹** `waitforddaman-prod-sg`: 인바운드 22(SSH, 관리자 IP만) / 80(웹, 전체 공개) /
   8080(Airflow UI, 관리자 IP만). 5432/8000은 인바운드 규칙 없음(내부망 전용).
4. **키페어**: `waitforddaman-prod-key` (RSA, .pem).
5. **EC2 인스턴스**: Amazon Linux 2023, **반드시 arm64 AMI**(t4g.large가 Graviton),
   `t4g.large`, gp3 50GB, 위 보안그룹 + IAM 인스턴스 프로파일 연결.

## EC2 최초 부트스트랩

```bash
ssh -i ~/.ssh/waitforddaman-prod-key.pem ec2-user@<EC2_PUBLIC_IP>

# Docker 설치
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user   # 재접속해야 반영됨

# Docker Compose plugin (AL2023 저장소에 없어 GitHub 릴리스에서 arm64 바이너리 직접 설치)
sudo mkdir -p /usr/local/lib/docker/cli-plugins
LATEST=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -o '"tag_name": "[^"]*' | cut -d'"' -f4)
sudo curl -SL "https://github.com/docker/compose/releases/download/${LATEST}/docker-compose-linux-aarch64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# AWS CLI (deploy 스크립트가 ECR 로그인에 사용)
sudo dnf install -y aws-cli

# 배포 디렉터리
sudo mkdir -p /opt/waitforddaman && sudo chown ec2-user:ec2-user /opt/waitforddaman
```

로컬에서 compose 파일 복사:
```bash
scp -i ~/.ssh/waitforddaman-prod-key.pem docker-compose.prod.yml ec2-user@<EC2_PUBLIC_IP>:/opt/waitforddaman/
```

`/opt/waitforddaman/.env`를 서버에서 직접 작성(레포에는 절대 커밋하지 않음, `.env.example` 참고).
prod 전용 차이: `APP_ENV=aws`, `ICEBERG_CATALOG_TYPE=hadoop`, `SPARK_LOCAL_EXECUTION=true`,
`S3_ENDPOINT`/정적 AWS 키 미설정(인스턴스 IAM Role 사용).

## 최초 수동 배포

```bash
cd /opt/waitforddaman
export ECR_REGISTRY=<account-id>.dkr.ecr.ap-northeast-2.amazonaws.com
export IMAGE_TAG=latest
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin $ECR_REGISTRY
docker compose -f docker-compose.prod.yml up -d
```

postgres healthy 확인 후 1회 시드 스크립트 실행:
```bash
docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/bike_man/bikeman_seed_init.sql
```

## CI/CD (GitHub Actions)

이미지 push와 배포가 분리되어 있다. 계정 SCP가 MFA 없는 요청을 차단하고 IAM 역할 생성
권한도 없어서, GitHub Actions(무인 실행)에서 ECR로 직접 push할 방법이 없기 때문이다.

| 단계 | 실행 주체 | AWS 인증 |
|---|---|---|
| 테스트/린트/빌드 검증 | `ci.yml` (PR마다 자동) | 불필요 |
| 이미지 빌드 & ECR push | **로컬 수동** `./infra/push-images.sh` | `aws login` 세션 |
| EC2 배포 | `cd.yml` (production push 시 자동) | 불필요(SSH만) |

**로컬 이미지 push:**
```bash
aws login --profile console   # 브라우저 인증, refresh token으로 자동 갱신
./infra/push-images.sh        # 3개 이미지 arm64 빌드 + push
```
`aws login`은 MFA로 인증된 콘솔 세션 자격증명을 가져오므로 SCP 조건을 만족한다.
`aws configure`로 넣은 정적 액세스 키는 MFA가 없어서 ECR이 거부된다.

- `ci.yml`: PR마다 pytest(ingestion/staging/pipeline) + web lint/build + 3개 prod
  Dockerfile 빌드 검증(push 없음).
- `cd.yml`: `production` 브랜치 push 시 SSH로 EC2 접속해 `docker compose pull && up -d`.
  EC2는 인스턴스 IAM 역할로 ECR pull하므로 워크플로우에 AWS 자격증명이 필요 없다.
  `workflow_dispatch`로 수동 실행도 가능(이미지만 새로 올린 뒤 재배포할 때).
- 필요한 GitHub Secrets: `EC2_HOST`, `EC2_SSH_USER`, `EC2_SSH_PRIVATE_KEY` (3개).
- **주의**: `down -v`는 절대 CD/운영 스크립트에 넣지 않는다 — Postgres named volume이
  날아가면 Airflow 메타데이터 + bikeman/serving 데이터가 전부 소실된다.

### 나중에 OIDC 권한을 받으면
관리자가 IAM Identity Provider(`token.actions.githubusercontent.com`) + 역할을 만들어주면,
`cd.yml`에 `build-and-push` 잡을 되살려(OIDC 인증 → QEMU/buildx로 arm64 크로스 빌드 →
ECR push) 완전 자동화할 수 있다. 그때 `push-images.sh`는 삭제해도 된다.

## 알려진 제약 / 후속 작업

- 이 계정은 조직 SCP로 일부 AWS 서비스(ECR 등)가 막혀 있을 수 있음 — 계정 관리자 확인 필요.
- EC2 `t4g.large`(8GB) 제약으로 `bronze_ingest`/`silver_process` pool 동시성을 1로 낮춤 —
  반기 CSV(~700MB)급 파일 처리 시 실측 후 필요하면 driver 메모리도 추가 조정.
- EMR, serving RDS 이전은 별도 트랙(타 담당자) — 이 문서 범위 밖.
