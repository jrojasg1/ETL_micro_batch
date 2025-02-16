from etl.pipelines.tax_pipeline import TaxPipeline
from pyspark.sql import SparkSession
import logging

# Configurar sesión de Spark con descarga automática de JDBC
spark = SparkSession.builder \
    .appName("Batch-Pipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
    .getOrCreate()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info(" Iniciando el Pipeline ETL...")

    # Ejecutar el pipeline de impuestos
    pipeline = TaxPipeline(spark, logger)
    pipeline.run()
    pipeline.print_statistics()

    logger.info(" Pipeline ETL completado con éxito.")
