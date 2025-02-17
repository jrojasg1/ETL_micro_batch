# ETL_micro_batch con Airflow

##  Diagrama de Clases
A continuación se incluye el diagrama de clases que muestra la estructura y relaciones entre las clases en el proyecto:
![UML](https://github.com/user-attachments/assets/d14ee4e0-088a-4039-92b1-9a3bc764dfee)

## Estructura del Proyecto

```plaintext
ETL_micro_batch/
├── README.md                  # Documentación del proyecto
├── airflow/
│   ├── dags/
│   │   ├── tax_etl_dag.py          # Configuración general del DAG
│ 
├── etl/
│   ├── extract/
│   │   ├── extract_base.py          # Clase base para extracción
│   │   ├── extract_txt.py           # Extrae datos desde archivos .TXT en micro batches
│   │   ├── extract_postgres.py      # Extrae datos desde PostgreSQL
│   ├── transform/
│   │   ├── tax_calculation.py       # Cálculo de tasas de impuestos
│   ├── load/
│   │   ├── load_base.py             # Clase base de carga
│   │   ├── load_postgres.py         # Carga datos en PostgreSQL
│   ├── pipelines/
│   │   ├── tax_pipeline.py          # Pipeline completo de tasas de impuestos
├── data/
│   ├── datasets.txt                  # Archivos .TXT crudos
│   ├── other.csv                     # Archivos de referencia (tarifas, mínimos, máximos)
├── scripts/
│   ├── init-db.sh                   # Script de inicialización de la base de datos
├── main.py                          # Punto de entrada principal
├── docker-compose.yaml              # Configuración de contenedores Docker
├── requirements.txt                 # Dependencias del proyecto
├── example.env                      # Variables de entorno

```
## Configuración

Clona el repositorio:
```bash
git clone https://github.com/jrojasg1/ETL_micro_batch.git
```
configura el entorno copiando example.env a .env y actualiza las variables necesarias.
Construir la imagen de Docker para Airflow:
```bash
docker-compose up --build
```
Levanta los contenedores con Docker:
```bash
docker-compose up -d
```
Después de iniciar los contenedores, abre en tu navegador: http://localhost:port:
