from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "tax_etl_pipeline",
    default_args=default_args,
    description="ETL para procesar impuestos con microbatches",
    schedule_interval="*/10 * * * *",  # Cada 10 minutos
    catchup=False,
)

spark_etl_task = SparkSubmitOperator(
    task_id="run_tax_etl",
    application="/opt/airflow/etl/main.py",  # Ruta en el contenedor de Airflow
    conn_id="spark_default",  # Definido en docker-compose.yaml
    executor_cores=2,
    executor_memory="2g",
    driver_memory="1g",
    verbose=True,
    dag=dag,
)

spark_etl_task
