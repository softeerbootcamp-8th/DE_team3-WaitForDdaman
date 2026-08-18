# DAG 설계 원칙 및 전체 파이프라인 개요

## 전체 DAG 그래프

```
bronze_daily_batch_all_sources_dag (Bronze 원천 수집)
    -> silver_*_daily (Silver 정제, 각 소스별)
        -> dag_gold_dim_fact (Gold dim/fact: dim_bike, bike_location, station_active, fact_station_inventory)
            -> dag_risk_decision (위험도 추론 + 대여중단 결정: fact_bike_risk, fact_bike_decision)
                -> gold_to_serving_sync (Gold mart -> Postgres 서빙: station_daily, bike_risk_daily)
```

## gold_to_serving_sync

- **트리거**: `dag_risk_decision`의 마지막 태스크(`build_fact_bike_decision`)가 성공하면
  `TriggerDagRunOperator(wait_for_completion=False)`로 트리거된다. 자체 스케줄은 없다
  (`schedule=None`).
- **대상 파티션**: `{{ ds }}` (트리거한 DAG run의 logical_date를 그대로 물려받음).
- **왜 dag_gold_dim_fact가 아니라 dag_risk_decision에서 트리거하는가**: `bike_risk_daily`
  마트가 필요로 하는 `gold.fact_bike_risk`/`gold.fact_bike_decision`은
  `dag_risk_decision`의 산출물이다. `dag_gold_dim_fact`가 끝난 시점에는 아직 이 두
  테이블이 없으므로, 두 마트(station_daily 포함) 모두 만들 재료가 갖춰지는 시점은
  `dag_risk_decision` 완료 시점이다.
- **태스크 구조**: `build_mart_*`(Gold join/집계 -> Iceberg mart 파티션 OVERWRITE) ->
  `write_*`(그 파티션 collect -> Postgres UPSERT) -> `verify_*_sync`(row count 비교)
  3단계를 원자적인 별도 태스크로 분리. `bike_risk_daily`/`station_daily` 두 브랜치는
  서로 의존하지 않아 병렬 실행.
- **실패 전파**: 트리거가 `wait_for_completion=False`라 이 DAG의 실패는
  `dag_risk_decision`에 전파되지 않는다 (이미 만들어진 gold 데이터 자체는 유효하므로).
  각 태스크는 실패 시 Slack Incoming Webhook으로 알림.
- **멱등성**: `build_mart_*`는 dynamic partition overwrite(같은 날짜 재실행 = 같은
  파티션 덮어쓰기), `write_*`는 `INSERT ... ON CONFLICT DO UPDATE`. 동일 `{{ ds }}`로
  여러 번 재실행해도 중복 행이 생기지 않는다.
