# serving_sync RDS 적재/검증 Lambda 이미지 (#172)
#
# 이미지 1개를 write_bike_risk_daily / write_station_daily / verify_serving_sync
# 3개 Lambda 함수가 공유한다 - Terraform의 image_config.command만 함수별로 다르게
# 줘서(예: "app.write_bike_risk_daily.handler") 이 이미지 안의 다른 진입점을 고른다.
# 빌드는 한 번, 함수(리소스)는 셋 - 함수별로 예약 동시성을 따로 걸 수 있으면서도
# 이미지 관리는 하나로 끝난다.
#
# 빌드 컨텍스트가 저장소 루트여야 한다(config/, ingestion/common/, pipeline/serving_sync/jobs/를
# 함께 COPY하므로) - 이 Dockerfile이 있는 디렉터리가 아니라 루트에서 실행:
#   docker build -f infra/lambdas/serving_sync/Dockerfile -t serving-sync-lambda .

FROM public.ecr.aws/lambda/python:3.11

# 실제로 쓰는 것만 최소로 복사한다 - iceberg_catalog.py는 config만 필요로 하고
# (common/__init__.py가 없는 네임스페이스 패키지라 다른 common/* 모듈을 끌어오지
# 않음), config.SETTINGS도 watermark_keys.py는 이 3개 잡이 안 쓰지만 config
# 패키지 자체가 import하지 않으므로 __init__.py만으로 충분하다.
COPY src/config/ ${LAMBDA_TASK_ROOT}/config/
COPY src/common/ ${LAMBDA_TASK_ROOT}/common/

# serving 잡 본체 - 로컬과 동일한 패키지 경로를 유지한다.
COPY src/serving/ ${LAMBDA_TASK_ROOT}/serving/

# Lambda 진입점(app.<모듈>.handler) - Secrets Manager에서 DB 자격증명을 채운 뒤
# 위 잡의 run()/verify_counts()를 그대로 부르는 얇은 래퍼.
COPY infra/lambdas/serving_sync/app ${LAMBDA_TASK_ROOT}/app

COPY infra/lambdas/serving_sync/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Terraform의 image_config.command가 함수별로 덮어쓴다 - 이건 기본값일 뿐.
CMD ["app.write_bike_risk_daily.handler"]
