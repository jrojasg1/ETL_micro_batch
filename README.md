# ETL_micro_batch

## Descripción de Componentes

- **airflow/**: Contiene la configuración de los DAGs de Apache Airflow.
  - **dags/**: Carpeta donde se definen los flujos de trabajo de ETL.

- **etl/**: Implementación del proceso ETL.
  - **extract/**: Módulo responsable de la extracción de datos.
  - **transform/**: Módulo donde se realizan las transformaciones necesarias sobre los datos.
  - **load/**: Módulo encargado de cargar los datos transformados en el destino.
  - **pipelines/**: Definición de pipelines completos.

- **data/**: Carpeta que contiene los datos de entrada y referencia utilizados en el proceso ETL.

- **scripts/**: Contiene scripts útiles, como la inicialización de la base de datos.

- **main.py**: Punto de entrada del proyecto.

- **docker-compose.yaml**: Archivo para configurar y orquestar contenedores Docker.

- **requirements.txt**: Lista de dependencias necesarias para el proyecto.

- **example.env**: Archivo de ejemplo con variables de entorno necesarias para la configuración.

## Instalación

1. Clona el repositorio:
   ```bash
   git clone <URL_DEL_REPOSITORIO>
