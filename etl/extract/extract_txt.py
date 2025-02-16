from etl.extract.extract_base import ExtractBase
from pyspark.sql import functions as F

class ExtractTXT(ExtractBase):
    def extract(self, source: str, has_quotes: bool = False, batch_size: int = 1000):
        """
        Extrae datos desde un archivo TXT en microbatches optimizados.

        :param source: Ruta del archivo TXT.
        :param has_quotes: Indica si el archivo tiene comillas dobles.
        :param batch_size: Tamaño del microbatch.
        :return: Generador que devuelve DataFrames por lotes.
        """
        self.logger.info(f"Extracting data from {source} in microbatches of {batch_size}...")

        read_options = {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ","
        }

        if has_quotes:
            read_options["quote"] = '"'
            read_options["escape"] = '"'

        # Lectura del archivo y particionamiento
        df = self.spark.read.options(**read_options).csv(source)
        df = df.withColumn("index", F.monotonically_increasing_id())
        
        # Definir el número de particiones de manera dinámica si es necesario
        num_partitions = max(df.rdd.getNumPartitions(), 10)  # Ejemplo, mínimo 10 particiones
        df = df.repartition(num_partitions)
        
        # Generación de microbatches
        total_rows = df.count()
        self.logger.info(f"Total rows in {source}: {total_rows}")
        
        for start in range(0, total_rows, batch_size):
            self.logger.info(f"Processing batch: {start} to {start + batch_size}")
            yield df.filter((F.col("index") >= start) & (F.col("index") < start + batch_size)).drop("index")
