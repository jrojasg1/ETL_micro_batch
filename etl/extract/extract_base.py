from pyspark.sql import SparkSession
import logging

class ExtractBase:
    """
    Clase base para todas las operaciones de extracción de datos en el pipeline ETL.
    Proporciona una estructura común y maneja la sesión de Spark y logs.
    """

    def __init__(self, spark: SparkSession, logger: logging.Logger):
        """
        Constructor de la clase ExtractBase.

        :param spark: Objeto de sesión de Spark para manejar DataFrames.
        :param logger: Objeto de logger para registrar eventos y errores.
        """
        self.spark = spark
        self.logger = logger

    def extract(self, source: str):
        """
        Método abstracto para la extracción de datos. Debe ser implementado en subclases.

        :param source: Ruta del archivo o conexión a la base de datos.
        :return: DataFrame de Spark con los datos extraídos.
        """
        raise NotImplementedError("El método extract() debe ser implementado en las subclases.")

