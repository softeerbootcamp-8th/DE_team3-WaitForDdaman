# ============================================================================
# Airflow 컨테이너용 Dockerfile
#
# 이 이미지는 x-airflow-common 앵커를 통해 airflow-apiserver, airflow-scheduler,
# airflow-dag-processor, airflow-triggerer, airflow-init 컨테이너가 전부 공유한다.
# 하지만 실제로 Spark(PySpark)를 실행하는 건 이 중 "airflow-scheduler" 하나뿐이다 -
# LocalExecutor가 태스크를 스케줄러 프로세스의 서브프로세스로 실행하기 때문이다
# ============================================================================

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ----------------------------------------------------------------------------
# 1) Java(JVM) 설치
# ----------------------------------------------------------------------------
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/current-java
 
ENV JAVA_HOME=/usr/lib/jvm/current-java

USER airflow

# ----------------------------------------------------------------------------
# 2) Airflow 자체 의존성
# ----------------------------------------------------------------------------
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ----------------------------------------------------------------------------
# 3) ingestion 파이프라인 의존성
# ----------------------------------------------------------------------------
COPY ingestion/requirements.txt /ingestion-requirements.txt
RUN pip install --no-cache-dir -r /ingestion-requirements.txt