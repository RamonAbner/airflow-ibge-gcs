import pendulum
from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyDatasetOperator,
)
from app.ibge_to_gcs import ibge_to_gcs
from config.settings import (
    TIMEZONE,
    SCHEDULE,
    BQ_PROJECT,
    BQ_DATASET_RAW,
    BQ_DATASET_BRONZE,
    BUCKET_NAME,
    GCP_CONN_ID,
    BQ_LOCATION,
)

# ==================================================
# SQL — RAW 
# ==================================================
SQL_CREATE_MUNICIPIOS_RAW = """
CREATE OR REPLACE EXTERNAL TABLE
  `{{ params.project }}.{{ params.dataset_raw }}`.municipios
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{{ params.bucket }}/ibge/municipios/{{params.year}}/{{params.month}}/{{params.day}}/*']
);
"""

SQL_CREATE_ESTADOS_RAW = """
CREATE OR REPLACE EXTERNAL TABLE
  `{{ params.project }}.{{ params.dataset_raw }}`.estados
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{{ params.bucket }}/ibge/estados/{{params.year}}/{{params.month}}/{{params.day}}/*']
);
"""

# ==================================================
# SQL — BRONZE 
# ==================================================
SQL_CREATE_BRONZE_MUNICIPIOS = f"""
CREATE OR REPLACE TABLE
  `{{{{ params.project }}}}.{{{{ params.dataset_bronze }}}}.municipios`
PARTITION BY ingestion_date AS
SELECT DISTINCT
  e.*,
  DATE('{{{{ ds }}}}') AS ingestion_date
FROM `{{{{ params.project }}}}.{{{{ params.dataset_raw }}}}.municipios` e;
"""

SQL_CREATE_BRONZE_ESTADOS = f"""
CREATE OR REPLACE TABLE
  `{{{{ params.project }}}}.{{{{ params.dataset_bronze }}}}.estados`
PARTITION BY ingestion_date AS
SELECT DISTINCT
  e.*,
  DATE('{{{{ ds }}}}') AS ingestion_date
FROM `{{{{ params.project }}}}.{{{{ params.dataset_raw }}}}.estados` e;
"""

now = datetime.datetime.now()


# ==================================================
# DAG
# ==================================================
with DAG(
    dag_id="ibge_to_gcs",
    start_date=pendulum.datetime(2025, 10, 1, tz=TIMEZONE),
    schedule=SCHEDULE,
    catchup=False,
    default_args={"retries": 2},
    tags=["ibge", "gcs", "bq", "raw", "bronze"],
    params={
        "project": BQ_PROJECT,
        "dataset_raw": BQ_DATASET_RAW,
        "dataset_bronze": BQ_DATASET_BRONZE,
        "bucket": BUCKET_NAME,
        "gcp_conn_id": GCP_CONN_ID,
        "year": f'year={now.year}',
        "month": f'month={now.month}',
        "day": f'day={now.day}',
    },
) as dag:

    # ==================================================
    # Criação de datasets
    # ==================================================
    ensure_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_raw_dataset",
        project_id=BQ_PROJECT,
        dataset_id=BQ_DATASET_RAW,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    ensure_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_bronze_dataset",
        project_id=BQ_PROJECT,
        dataset_id=BQ_DATASET_BRONZE,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    # ==================================================
    # Ingestão GCS
    # ==================================================
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

    # ==================================================
    # Criação RAW 
    # ==================================================
    create_estados_raw = BigQueryInsertJobOperator(
        task_id="create_estados_raw",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": SQL_CREATE_ESTADOS_RAW, "useLegacySql": False}},
    )

    create_municipios_raw = BigQueryInsertJobOperator(
        task_id="create_municipios_raw",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": SQL_CREATE_MUNICIPIOS_RAW, "useLegacySql": False}},
    )

    # ==================================================
    # Criação BRONZE 
    # ==================================================
    create_bronze_estados = BigQueryInsertJobOperator(
        task_id="create_estados_bronze",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": SQL_CREATE_BRONZE_ESTADOS, "useLegacySql": False}},
    )

    create_bronze_municipios = BigQueryInsertJobOperator(
        task_id="create_municipios_bronze",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": SQL_CREATE_BRONZE_MUNICIPIOS, "useLegacySql": False}},
    )

    [ensure_raw_dataset, ensure_bronze_dataset] >> ingest_estados >> create_estados_raw >> create_bronze_estados
    [ensure_raw_dataset, ensure_bronze_dataset] >> ingest_municipios >> create_municipios_raw >> create_bronze_municipios
