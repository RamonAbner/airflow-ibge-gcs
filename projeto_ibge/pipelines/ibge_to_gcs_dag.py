# dags/pipelines/ibge_to_gcs_dag.py
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyDatasetOperator,
)
from ingestion.ibge_to_gcs import ibge_to_gcs
from config.settings import (
    TIMEZONE,
    SCHEDULE,
    BQ_PROJECT,
    BQ_DATASET_BRONZE,
    BUCKET_NAME,
    GCP_CONN_ID,
    BQ_LOCATION,
)

# -----------------------------
# SQLs de BRONZE (a partir de STAGING) com dedup por id
# -----------------------------

BRONZE_ESTADOS_FROM_STG_SQL = """
-- 1) Garante a BRONZE (vazia) com o schema da STAGING + ingestion_date
CREATE TABLE IF NOT EXISTS
  `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_estados
PARTITION BY ingestion_date AS
SELECT
  stg.*,
  DATE('{{ ds }}') AS ingestion_date
FROM `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_estados_stg stg
WHERE 1 = 0;

-- 2) Reconstrói a BRONZE = BRONZE atual ∪ STAGING do dia, dedup por id
CREATE OR REPLACE TABLE
  `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_estados
PARTITION BY ingestion_date AS
WITH
  base AS (
    SELECT * FROM `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_estados
  ),
  src AS (
    SELECT
      stg.*,
      DATE('{{ ds }}') AS ingestion_date
    FROM `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_estados_stg stg
  ),
  unioned AS (
    SELECT * FROM base
    UNION ALL
    SELECT * FROM src
  ),
  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        unioned.*,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) AS rn
      FROM unioned
    )
    WHERE rn = 1
  )
SELECT * FROM dedup;
"""

BRONZE_MUNICIPIOS_FROM_STG_SQL = """
-- 1) Garante a BRONZE (vazia) com o schema da STAGING + ingestion_date
CREATE TABLE IF NOT EXISTS
  `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_municipios
PARTITION BY ingestion_date AS
SELECT
  stg.*,
  DATE('{{ ds }}') AS ingestion_date
FROM `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_municipios_stg stg
WHERE 1 = 0;

-- 2) Reconstrói a BRONZE = BRONZE atual ∪ STAGING do dia, dedup por id
CREATE OR REPLACE TABLE
  `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_municipios
PARTITION BY ingestion_date AS
WITH
  base AS (
    SELECT * FROM `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_municipios
  ),
  src AS (
    SELECT
      stg.*,
      DATE('{{ ds }}') AS ingestion_date
    FROM `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_municipios_stg stg
  ),
  unioned AS (
    SELECT * FROM base
    UNION ALL
    SELECT * FROM src
  ),
  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        unioned.*,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) AS rn
      FROM unioned
    )
    WHERE rn = 1
  )
SELECT * FROM dedup;
"""

# -----------------------------
# Definição da DAG
# -----------------------------

with DAG(
    dag_id="ibge_to_gcs",
    start_date=pendulum.datetime(2025, 10, 1, tz=TIMEZONE),
    schedule=SCHEDULE,
    catchup=False,
    default_args={"retries": 2},
    tags=["ibge", "gcs", "bq", "bronze", "no-external"],
    params={
        "project": BQ_PROJECT,
        "dataset_bronze": BQ_DATASET_BRONZE,
        "bucket": BUCKET_NAME,
        "gcp_conn_id": GCP_CONN_ID,
    },
) as dag:

    # 0) Garante o dataset da Bronze existir (evita 404)
    ensure_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_bronze_dataset",
        project_id=BQ_PROJECT,
        dataset_id=BQ_DATASET_BRONZE,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    # 1) Ingestão: API -> Parquet (GCS) com colunas saneadas (ponto, espaço, hífen)
    ingest_estados = PythonOperator(
        task_id="ingest_estados_to_gcs",
        python_callable=ibge_to_gcs,
        op_kwargs={"table_path": "localidades/estados", "dt": "{{ ds }}"},
    )
    ingest_municipios = PythonOperator(
        task_id="ingest_municipios_to_gcs",
        python_callable=ibge_to_gcs,
        op_kwargs={"table_path": "localidades/municipios", "dt": "{{ ds }}"},
    )

    # 2) LOAD: Parquet -> STAGING (tabelas *_stg)
    load_estados_stg = BigQueryInsertJobOperator(
        task_id="load_estados_stg",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": [
                    "gs://{{ params.bucket }}/ibge/estados/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/*.parquet"
                ],
                "destinationTable": {
                    "projectId": "{{ params.project }}",
                    "datasetId": "{{ params.dataset_bronze }}",
                    "tableId": "tb_ibge_estados_stg",
                },
                "sourceFormat": "PARQUET",
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    load_municipios_stg = BigQueryInsertJobOperator(
        task_id="load_municipios_stg",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "load": {
                "sourceUris": [
                    "gs://{{ params.bucket }}/ibge/municipios/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/*.parquet"
                ],
                "destinationTable": {
                    "projectId": "{{ params.project }}",
                    "datasetId": "{{ params.dataset_bronze }}",
                    "tableId": "tb_ibge_municipios_stg",
                },
                "sourceFormat": "PARQUET",
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    # 3) BRONZE: acumula BRONZE atual + STAGING do dia e dedup por id
    bronze_estados = BigQueryInsertJobOperator(
        task_id="bronze_estados",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": BRONZE_ESTADOS_FROM_STG_SQL,
                "useLegacySql": False,
            }
        },
    )

    bronze_municipios = BigQueryInsertJobOperator(
        task_id="bronze_municipios",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": BRONZE_MUNICIPIOS_FROM_STG_SQL,
                "useLegacySql": False,
            }
        },
    )

    # 4) (Opcional) Limpa as STAGING
    drop_stg_estados = BigQueryInsertJobOperator(
        task_id="drop_stg_estados",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": "DROP TABLE `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_estados_stg;",
                "useLegacySql": False,
            }
        },
        trigger_rule="all_done",
    )

    drop_stg_municipios = BigQueryInsertJobOperator(
        task_id="drop_stg_municipios",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": "DROP TABLE `{{ params.project }}.{{ params.dataset_bronze }}`.tb_ibge_municipios_stg;",
                "useLegacySql": False,
            }
        },
        trigger_rule="all_done",
    )

    # Encadeamentos
    ensure_bronze_dataset >> ingest_estados >> load_estados_stg >> bronze_estados >> drop_stg_estados
    ensure_bronze_dataset >> ingest_municipios >> load_municipios_stg >> bronze_municipios >> drop_stg_municipios
