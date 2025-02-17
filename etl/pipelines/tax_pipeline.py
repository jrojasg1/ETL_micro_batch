from etl.extract.extract_txt import ExtractTXT
from etl.transform.tax_calculation import calculate_tax
from etl.load.load_postgres import LoadPostgres
import time
import os
from dotenv import load_dotenv
import psycopg2

# Cargar variables de entorno desde el archivo .env
load_dotenv()

class TaxPipeline:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
        self.extractor = ExtractTXT(spark, logger)
        self.loader = LoadPostgres(spark, logger)

        # Rutas de los archivos
        self.tax_rates_file = os.getenv("TAX_RATES_FILE")
        self.min_values_file = os.getenv("MIN_VALUES_FILE")
        self.max_values_file = os.getenv("MAX_VALUES_FILE")
        self.dataset_dir = os.getenv("DATASET_FOLDER")  # Carpeta con múltiples archivos

        # Validar que las rutas están configuradas
        if not all([self.tax_rates_file, self.min_values_file, self.max_values_file, self.dataset_dir]):
            raise ValueError(" Error: Faltan variables de entorno para las rutas de los archivos.")

    def run(self):
        self.logger.info("Running Tax ETL pipeline...")

        # Extraer tarifas y valores una sola vez
        try:
            start_time = time.time()
            tax_rates = next(self.extractor.extract(self.tax_rates_file, has_quotes=True))
            min_values = next(self.extractor.extract(self.min_values_file, has_quotes=True))
            max_values = next(self.extractor.extract(self.max_values_file, has_quotes=True))
            extraction_time = time.time() - start_time
            self.logger.info(f"Extracted tax rates and limits in {extraction_time:.2f} seconds.")
        except Exception as e:
            self.logger.error(f" Error during extraction of rates/limits: {e}")
            return

        # 📌 **Procesar todos los archivos dentro de la carpeta `dataset_dir`**
        dataset_files = [os.path.join(self.dataset_dir, f) for f in os.listdir(self.dataset_dir) if f.endswith(".txt")]

        if not dataset_files:
            self.logger.warning(" No dataset files found in directory.")
            return

        # Procesar cada archivo TXT**
        for file_path in dataset_files:
            self.logger.info(f"Processing file: {file_path}")

            # Procesar en microbatches
            count = 1
            try:
                for df_batch in self.extractor.extract(file_path, has_quotes=False, batch_size=1000):
                    self.logger.info(f"Processing batch {count} from {file_path}...")

                    if df_batch is None or df_batch.isEmpty():
                        self.logger.warning(f"Empty batch encountered in {file_path}, skipping...")
                        continue

                    # Transformación
                    start_transform_time = time.time()
                    df_transformed = calculate_tax(df_batch, tax_rates, min_values, max_values)
                    transformation_time = time.time() - start_transform_time
                    self.logger.info(f"Batch {count} from {file_path} transformed in {transformation_time:.2f} seconds.")

                    # Carga en BD
                    start_load_time = time.time()
                    self.loader.load(df_transformed, "taxes", "tracking_table")
                    load_time = time.time() - start_load_time
                    self.logger.info(f"Batch {count} from {file_path} loaded into DB in {load_time:.2f} seconds.")

                    count += 1
            except Exception as e:
                self.logger.error(f" Error processing {file_path}: {e}")

    def print_statistics(self):
        """
        Consulta en la base de datos las estadísticas de los impuestos calculados.
        """
        try:
            conn = psycopg2.connect(
                host=os.getenv("HOST_DB", "postgres"),  # Usar el nombre del servicio en Docker
                port=os.getenv("HOST_PORT", "5432"),      # Especificar el puerto
                database=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
            cursor = conn.cursor()

            # Consulta SQL para obtener estadísticas
            cursor.execute("""
                SELECT 
                    COUNT(*) AS total_rows,
                    AVG(tax_calculada) AS avg_tax,
                    SUM(tax_calculada) AS sum_tax,
                    MIN(tax_calculada) AS min_tax,
                    MAX(tax_calculada) AS max_tax
                FROM taxes;
            """)

            # Obtener los resultados
            result = cursor.fetchone()
            self.logger.info(f"Statistics - Total rows: {result[0]}, Avg tax: {result[1]}, Sum tax: {result[2]}, Min tax: {result[3]}, Max tax: {result[4]}")

            # Cerrar la conexión
            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"Error fetching statistics: {e}")
