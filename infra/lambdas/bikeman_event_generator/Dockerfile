# bikeman_event_generator RDS 적재 Lambda 이미지 (#186)
#
# 이미지 1개를 generate_collect_events / deploy_returned_bikes 2개 Lambda 함수가
# 공유한다 - Terraform의 image_config.command만 함수별로 다르게 줘서(예:
# "app.generate_collect_events.handler") 이 이미지 안의 다른 진입점을 고른다.
#
# 빌드 컨텍스트가 저장소 루트여야 한다(pipeline/bikeman_event_generator/jobs/를
# 함께 COPY하므로) - 이 Dockerfile이 있는 디렉터리가 아니라 루트에서 실행:
#   docker build -f infra/lambdas/bikeman_event_generator/Dockerfile -t bikeman-event-generator-lambda .

FROM public.ecr.aws/lambda/python:3.11

# bikeman_event_generator 잡 본체 - 로컬 실행과 완전히 같은 코드를 그대로 쓴다.
COPY pipeline/bikeman_event_generator/jobs/bikeman_db.py ${LAMBDA_TASK_ROOT}/bikeman_db.py
COPY pipeline/bikeman_event_generator/jobs/event_builder.py ${LAMBDA_TASK_ROOT}/event_builder.py
COPY pipeline/bikeman_event_generator/jobs/event_ids.py ${LAMBDA_TASK_ROOT}/event_ids.py
COPY pipeline/bikeman_event_generator/jobs/generate_collect_events.py ${LAMBDA_TASK_ROOT}/generate_collect_events.py
COPY pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py ${LAMBDA_TASK_ROOT}/deploy_returned_bikes.py

# Lambda 진입점(app.<모듈>.handler) - Secrets Manager에서 DB 자격증명을 채운 뒤
# 위 잡의 run()을 그대로 부르는 얇은 래퍼.
COPY infra/lambdas/bikeman_event_generator/app ${LAMBDA_TASK_ROOT}/app

COPY infra/lambdas/bikeman_event_generator/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Terraform의 image_config.command가 함수별로 덮어쓴다 - 이건 기본값일 뿐.
CMD ["app.generate_collect_events.handler"]