from pyspark.sql import functions as F
import uuid
from pyspark.sql.types import DoubleType, IntegerType


def clean_dataframes(df, df_tarifas, df_minimos, df_maximos):
    """
    Limpia y formatea los datos antes del cálculo de impuestos.
    """

    #  Optimización: Evitar múltiples accesos a columnas con select()
    df = df.select(
        F.col("id").alias("ID"),
        F.col("año").cast(IntegerType()).alias("Año"),
        F.col("destino").alias("Destino"),
        F.when(F.col("estrato").isNull(), 0).otherwise(F.col("estrato").cast(IntegerType())).alias("Estrato"),
        F.col("consumo").cast(DoubleType()).alias("Consumo")
    )
    df = df.repartition(df.rdd.getNumPartitions()) # Reparticionar para mejorar rendimiento

    # Limpiar Tarifa_sobre_consumo
    df_tarifas = df_tarifas.withColumn(
    "Tarifa_sobre_consumo",
    F.regexp_replace(F.col("Tarifa_sobre_consumo"), '[^0-9,]', "")  # Quitar todo menos números y comas
    .alias("Tarifa_sobre_consumo")
    )

    df_tarifas = df_tarifas.withColumn(
        "Tarifa_sobre_consumo",
        F.regexp_replace(F.col("Tarifa_sobre_consumo"), ",", ".").cast(DoubleType()) / 100
    )

    # Limpiar valores de Mínimo y Máximo
    df_minimos = df_minimos.withColumn(
        "Mínimo",
        F.regexp_replace(F.col("Mínimo"), "[,.]", "").cast(DoubleType())
    )

    df_maximos = df_maximos.withColumn(
        "Máximo",
        F.regexp_replace(F.col("Máximo"), "[,.]", "").cast(DoubleType())
    )

    # Llenar valores nulos en Estrato y Tarifas
    df_tarifas = df_tarifas.fillna({"Estrato": 0})
    df_minimos = df_minimos.fillna({"Mínimo": 0})
    df_maximos = df_maximos.fillna({"Máximo": 0})

    return df, df_tarifas, df_minimos, df_maximos


def calculate_tax(df, tax_rates, min_values, max_values):
    df, tax_rates, min_values, max_values = clean_dataframes(df, tax_rates, min_values, max_values)
    
    # Unir DataFrames usando broadcast si son pequeños
    df = df.join(F.broadcast(tax_rates), on=["Destino", "Estrato"], how="left") \
           .join(F.broadcast(min_values), on="Año", how="left") \
           .join(F.broadcast(max_values), on=["Año", "Destino"], how="left") \
           .fillna({"Tarifa_sobre_consumo": 0, "Mínimo": 0, "Máximo": 0})

    # Cálculo de la tasa de impuestos con aplicación de mínimos y máximos
    df = df.withColumn("calculated_tax", F.col("Tarifa_sobre_consumo") * F.col("Consumo"))
    df = df.withColumn(
        "calculated_tax",
        F.when(F.col("calculated_tax") < F.col("Mínimo"), F.col("Mínimo"))
         .when(F.col("calculated_tax") > F.col("Máximo"), F.col("Máximo"))
         .otherwise(F.col("calculated_tax"))
    )

    # Renombrar columnas y agregar UUID
    new_column_names = ["anio", "destino", "estrato", "registro_id", "consumo", "tarifa_sobre_consumo", "minimo", "maximo", "tax_calculada"]
    df = df.toDF(*new_column_names)
    df = df.withColumn("id", F.expr("uuid()"))

    return df
