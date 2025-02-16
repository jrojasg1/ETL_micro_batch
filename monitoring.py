import psycopg2
import os

# Cargar credenciales desde variables de entorno
DB_NAME = os.getenv("POSTGRES_DB", "batch_pipeline")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "securepassword")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def monitor_database():
    try:
        # Conectar a PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Contar registros en la tabla "taxes"
        cursor.execute("SELECT COUNT(*) FROM taxes;")
        row_count = cursor.fetchone()[0]

        # Sumar el total de las tasas calculadas
        cursor.execute("SELECT SUM(calculated_tax) FROM taxes;")
        total_tax = cursor.fetchone()[0]

        print("\n **Monitoreo de la Base de Datos** 📊")
        print(f" Registros cargados: {row_count}")
        print(f" Total de impuestos calculados: {total_tax}\n")

        # Cerrar conexión
        cursor.close()
        conn.close()

    except Exception as e:
        print(f" Error al conectar a la base de datos: {e}")

if __name__ == "__main__":
    print("🔍 Ejecutando monitoreo de la base de datos...")
    monitor_database()
