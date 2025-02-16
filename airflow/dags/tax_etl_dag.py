from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.pipelines.tax_pipeline import TaxPipeline
from etl.utils.logging_config import setup_logger
from pyspark.sql import SparkSession

# Configuración de Airflow
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

# Crear sesión de Spark
def init_spark():
    try:
        spark = SparkSession.builder \
            .appName("Batch-Pipeline") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Error al iniciar Spark: {e}")
        raise 

# Definir tarea de ejecución del pipeline
def run_etl():
    """
    Ejecuta el pipeline ETL para procesar los datos de impuestos.
    1. Inicializa la sesión de Spark.
    2. Configura el logger.
    3. Ejecuta el pipeline ETL con la clase TaxPipeline.
    """
    try:
        spark = init_spark()
        logger = setup_logger()
        pipeline = TaxPipeline(spark, logger)
        pipeline.run()
    except Exception as e:
        logger.error(f"Error en el pipeline ETL: {e}")
        raise


etl_task = PythonOperator(
    task_id="run_tax_etl",
    python_callable=run_etl,
    dag=dag,
)

etl_task
