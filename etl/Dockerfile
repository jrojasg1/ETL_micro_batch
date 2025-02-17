# Usar una imagen oficial de Python como base
FROM python:3.8-slim

# Instalar dependencias de sistema necesarias para Spark
RUN apt-get update && apt-get install -y postgresql-client

# Copiar el archivo requirements.txt al contenedor
COPY ../requirements.txt /tmp/requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copiar todo el código del proyecto
COPY . /app
WORKDIR /app

# Comando para ejecutar el script Python
CMD ["python", "main.py"]
