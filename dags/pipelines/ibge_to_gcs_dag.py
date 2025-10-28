import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from ingestion.ibge_to_gcs import ibge_to_gcs
from config.settings import TIMEZONE, SCHEDULE

with DAG(
    dag_id="ibge_to_gcs_localidades",
    start_date=pendulum.datetime(2025, 10, 1, tz=TIMEZONE),
    schedule=SCHEDULE,
    catchup=False,
    default_args={"retries": 2},
    tags=["ibge", "gcs", "localidades"],
) as dag:

    load_estados = PythonOperator(
        task_id="load_estados",
        python_callable=ibge_to_gcs,
        op_kwargs={"table_path": "localidades/estados", "dt": "{{ ds }}"},
    )

    load_municipios = PythonOperator(
        task_id="load_municipios",
        python_callable=ibge_to_gcs,
        op_kwargs={"table_path": "localidades/municipios", "dt": "{{ ds }}"},
    )

    load_estados >> load_municipios
