import os
from dotenv import load_dotenv
from etl.load.load_base import LoadBase
from pyspark.sql import functions as F

# Cargar variables de entorno desde el archivo .env
env_file = os.getenv("ENV_FILE", ".env")

# Verificar si el archivo .env está presente
if not os.path.exists(env_file):
    raise FileNotFoundError(f" Error: El archivo de configuración {env_file} no se encuentra.")
load_dotenv(env_file)


class LoadPostgres(LoadBase):
    """
    Clase para cargar datos en PostgreSQL utilizando variables de entorno.
    """

    def __init__(self, spark, logger):
        """
        Constructor de la clase LoadPostgres.

        :param spark: Objeto de SparkSession.
        :param logger: Objeto de logger para registrar eventos y errores.
        """
        super().__init__(logger)  # Hereda el logger de LoadBase
        self.spark = spark  # Definir Spark en la clase hija

        # Cargar variables de entorno desde .env
        self.db_url = os.getenv("POSTGRES_DB_URL")
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_driver = os.getenv("DB_DRIVER")  # Valor por defecto

        # Validar que las credenciales están configuradas
        if not all([self.db_url, self.db_user, self.db_password]):
            self.logger.error(" Error: Faltan variables de entorno para la conexión a la base de datos.")
            raise ValueError(" Error: Faltan variables de entorno para la conexión a la base de datos.")

    def load(self, df, table_name, tracking_table):
        """
        Carga los datos en PostgreSQL y registra métricas de carga.

        :param df: DataFrame de Spark con los datos a cargar.
        :param table_name: Nombre de la tabla de destino.
        :param tracking_table: Tabla donde se registrarán las métricas de carga.
        """
        try:
            self.logger.info(f"Loading data into PostgreSQL table {table_name}...")

            #  Optimización: Usar agg para la suma de tasas
            sum_calculated_tax = df.agg(F.sum("tax_calculada")).collect()[0][0] or 0
            count_records = df.count()

            #  Crear DataFrame con las métricas de carga
            tracking_df = self.spark.createDataFrame(
                [(table_name, count_records, sum_calculated_tax)],
                ["table_name", "count_records", "sum_calculated_tax"]
            )

            #  Cargar datos en la tabla principal
            df.write \
                .format("jdbc") \
                .option("url", self.db_url) \
                .option("dbtable", table_name) \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .option("driver", self.db_driver) \
                .option("batchsize", "10000") \
                .mode("append") \
                .save()

            self.logger.info(f" Loaded {count_records} records into {table_name} (SUM: {sum_calculated_tax})")

            # Guardar métricas de carga en la tabla de seguimiento
            tracking_df.write \
                .format("jdbc") \
                .option("url", self.db_url) \
                .option("dbtable", tracking_table) \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .option("driver", self.db_driver) \
                .mode("append") \
                .save()

            self.logger.info(f" Metrics saved in {tracking_table}")

        except Exception as e:
            self.logger.error(f" Error during loading data: {e}")
            raise  # Propagar el error para que el proceso de ETL falle si algo sale mal
