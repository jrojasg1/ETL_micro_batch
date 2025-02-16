import logging
from pyspark.sql import DataFrame

class LoadBase:
    """
    Clase base para todas las operaciones de carga de datos en el pipeline ETL.
    Proporciona una estructura común y maneja logs.
    """

    def __init__(self, logger: logging.Logger):
        """
        Constructor de la clase LoadBase.

        :param logger: Objeto de logger para registrar eventos y errores.
        """
        self.logger = logger

    def load(self, df: DataFrame, destination: str):
        """
        Método abstracto para la carga de datos. Debe ser implementado en subclases.

        :param df: DataFrame de Spark con los datos procesados.
        :param destination: Destino donde se deben cargar los datos (puede ser una BD, S3, etc.).
        """
        raise NotImplementedError("El método load() debe ser implementado en las subclases.")
