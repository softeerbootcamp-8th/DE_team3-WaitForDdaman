# infra

프로덕션(EC2 + ECR + docker compose + GitHub Actions) 배포 설정과 절차.

## 이 브랜치에는 로컬 개발용 파일이 없다

`production` 브랜치는 배포 전용이라 dev/local 파일을 의도적으로 제거했다
(`docker-compose.yml`, `docker-compose.local.yml`, `airflow/Dockerfile`,
`airflow/Dockerfile.local`, `services/*/Dockerfile`, `config/risk_model.local.yaml`).
남은 건 `.prod` 계열뿐이다:

```
docker-compose.prod.yml
airflow/Dockerfile.prod
services/api/Dockerfile.prod
services/web/Dockerfile.prod
```

**로컬 개발은 `develop` / `develop-services` 브랜치에서 한다.** 각 모듈 README에
남아 있는 로컬 실행 설명(LocalStack, `docker compose -f docker-compose.local.yml` 등)은
그 브랜치 기준이다. 또 `production`은 단방향(develop → production)으로만 운용한다 —
이 브랜치를 develop으로 되머지하면 위 삭제가 전파되어 팀원들 로컬 환경이 깨진다.

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
`S3_ENDPOINT`/정적 AWS 키 미설정(인스턴스 IAM Role 사용), `AIRFLOW_DB_*`/`DOMAIN_DB_*`/
`SERVING_DB_*`/`BIKEMAN_DB_*`는 RDS 엔드포인트.

### .env 관리 방식

`.env` 파일이 두 개라 혼동하기 쉽다:

| 파일 | 용도 | git |
|---|---|---|
| `.env` (저장소 루트) | **로컬 개발용** (develop 브랜치 기준) | 무시됨 |
| `.env.prod` (저장소 루트) | **프로덕션 값의 source of truth** | 무시됨 |
| `/opt/waitforddaman/.env` (EC2) | 실제로 컨테이너가 읽는 파일 | - |
| `.env.example` | 템플릿(플레이스홀더만) | **추적됨** |

`.env.prod`를 편집한 뒤 아래로 반영한다. **서버에서 직접 편집하지 말 것** — 로컬과
갈려서 다음 sync에 덮어써진다.

```bash
./infra/sync-env.sh              # 반영만 (DAG가 source하는 값은 다음 태스크에 바로 적용)
./infra/sync-env.sh --restart    # compose environment: 블록 값을 바꿨을 때
```

스크립트가 하는 일: 플레이스홀더 미기입 검사 → scp → **권한 640 설정** → 컨테이너에서
실제로 읽히는지 확인.

**DB 비밀번호는 URL에 넣지 않는다:** `docker-compose.prod.yml`은 접속 URL에서 비밀번호를
빼고 `PGPASSWORD`로 따로 넘긴다. URL에 그대로 끼우면 `@` `/` `:` 가 들어간 비밀번호가
**에러 없이 다른 host/db로 파싱된다**(실측: `p@ss/w:rd#1` → host=`ss`). psycopg2가
libpq를 거치며 `PGPASSWORD`를 집어오므로 URL에서 빼도 정상 연결된다. 비밀번호를
교체할 때 특수문자를 피할 필요가 없어진다.

**권한이 왜 중요한가:** Airflow 컨테이너가 uid 50000 / **gid 0**으로 돌면서 이 파일을
`/opt/airflow/ingestion/.env`로 마운트해 `source`한다. `chmod 600`으로 두면 컨테이너가
읽지 못해 **모든 DAG 태스크가 "Permission denied"로 실패한다**(실제로 겪음). 그룹 root에
읽기 권한을 주면 컨테이너는 읽고 호스트의 다른 사용자는 못 읽는 상태가 된다. 수동 scp로는
이 단계를 빼먹기 쉬워서 스크립트로 굳혔다.

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

| 단계 | 실행 주체 | AWS 인증 |
|---|---|---|
| 테스트/린트/빌드 검증 | `ci.yml` (PR마다 자동) | 불필요 |
| 이미지 빌드 & ECR push | `cd.yml` (production push 시 자동) | OIDC 역할 assume |
| EC2 배포 | `cd.yml` (위와 같은 워크플로우) | 불필요(SSH만) |

- **`ci.yml`**: PR마다 pytest + web lint/build + 3개 prod Dockerfile 빌드 검증(push 없음).
  테스트는 모듈별로 cwd/PYTHONPATH를 맞춰서 실행해야 한다(각 모듈 테스트가 자기
  디렉터리 기준으로 import함). `PYSPARK_PYTHON`도 명시해야 Spark 워커/드라이버
  파이썬 버전 불일치로 안 죽는다.
- **`cd.yml`**: `production` push 시 ① OIDC로 `waitforddaman-gha-role` assume →
  QEMU/buildx로 3개 이미지 arm64 크로스 빌드 → ECR push(`:latest` + `:커밋SHA`),
  ② SSH로 EC2 접속해 해당 SHA 태그로 `docker compose pull && up -d`.
  `workflow_dispatch`로 수동 실행도 가능.
- 필요한 GitHub Secrets (4개): `AWS_OIDC_ROLE_ARN`, `EC2_HOST`, `EC2_SSH_USER`,
  `EC2_SSH_PRIVATE_KEY`.
- **주의**: `down -v`는 절대 CD/운영 스크립트에 넣지 않는다 — Postgres named volume이
  날아가면 Airflow 메타데이터 + bikeman/serving 데이터가 전부 소실된다.

### OIDC 인증 구성 (1회 생성, 완료됨)
```
IAM Identity Provider : token.actions.githubusercontent.com (audience: sts.amazonaws.com)
IAM Role              : waitforddaman-gha-role
  - trust  : 위 provider + sub가 정확히
             repo:softeerbootcamp-8th/DE_team3-WaitForDdaman:ref:refs/heads/production
             일 때만 assume 가능 (다른 브랜치/저장소는 실패)
  - policy : ecr-push      (인라인) ECR 인증 + waitforddaman-* 리포지토리 push/pull
             sg-ssh-manage (인라인) waitforddaman-prod-sg의 22번 인바운드 add/remove
```

### 러너 IP 임시 허용 (SSH 배포용)
보안그룹 22번은 팀 IP만 허용하는데 GitHub 러너 IP는 매 실행마다 바뀐다. 그래서
`deploy` 잡이 배포 직전 자기 IP(`/32`)만 열고, 끝나면 회수한다:

1. `checkip.amazonaws.com`으로 러너 공인 IP 조회
2. `ec2:AuthorizeSecurityGroupIngress`로 22번 개방
3. SSH 배포
4. `ec2:RevokeSecurityGroupIngress`로 회수 — **`if: always()`** 이므로 배포가 실패해도
   반드시 실행된다. 이게 없으면 실패한 실행의 IP가 SG에 영구히 남아 오염된다.

동시 실행 시 한쪽이 회수하는 사이 다른 쪽이 배포 중일 수 있어 `concurrency`로
배포를 직렬화한다(`group: production-deploy`, `cancel-in-progress: false`).

22번을 `0.0.0.0/0`으로 열거나 GitHub 공개 IP 대역 전체를 허용하는 방식은 쓰지 않는다.
GitHub Actions에 정적 AWS 키를 저장하지 않는 이유: 계정 SCP가 MFA 없는 IAM 사용자
요청을 차단하므로 정적 키로는 ECR이 거부된다. assumed-role 세션은 통과한다
(EC2 인스턴스 역할이 MFA 없이 ECR pull되는 것과 같은 이유).

### 로컬에서 직접 push해야 할 때
`./infra/push-images.sh`가 남아 있다(CD가 막혔을 때의 폴백, 또는 CD 없이 급하게
이미지만 갱신할 때). 사전에 `aws login --profile console`로 브라우저 인증이 필요하다 —
`aws configure`의 정적 키는 위 SCP 때문에 ECR이 거부된다.

## 알려진 제약 / 후속 작업

- 이 계정은 조직 SCP로 일부 AWS 서비스(ECR 등)가 막혀 있을 수 있음 — 계정 관리자 확인 필요.
- EC2 `t4g.large`(8GB) 제약으로 `bronze_ingest`/`silver_process` pool 동시성을 1로 낮춤 —
  반기 CSV(~700MB)급 파일 처리 시 실측 후 필요하면 driver 메모리도 추가 조정.
- EMR, serving RDS 이전은 별도 트랙(타 담당자) — 이 문서 범위 밖.
