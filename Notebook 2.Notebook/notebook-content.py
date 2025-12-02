# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "da392e62-49a0-4a27-82b4-7a1ce9c619e6",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "23fc4e08-257c-44ed-9f11-927b7ec4610d",
# META       "known_lakehouses": [
# META         {
# META           "id": "da392e62-49a0-4a27-82b4-7a1ce9c619e6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# SPARK en Notebooks
DataframeSpark = spark.read.table("Bronze.minciencia")

# Seleccionar solo las tres columnas requeridas
DataframeSpark = DataframeSpark.select(
    "time",
    "ff_Valor",
    "CodigoNacional"
)

display(DataframeSpark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Es preferible utilizar Spark porque distribuye y se puede escalar, a diferencia de Python.
import pandas as pd

# Lectura del archivo Parquet
DataframePandas = pd.read_parquet("abfss://Cane@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Tables/minciencia")

# Filtrado y selección de las columnas 'time', 'ff_Valor', y 'CodigoNacional'
# Se utiliza la notación de doble corchete [[]] para seleccionar múltiples columnas
DataframePandas = DataframePandas[['time', 'ff_Valor', 'CodigoNacional']]

display(DataframePandas)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# CELDAS DE IMPORTACIÓN DE LIBRERÍAS DE SPARK Y PANDAS

from pyspark.sql import functions as F
import pandas as pd
import pmdarima as pm # Necesaria aquí si no quieres otra celda de import
from datetime import timedelta
import numpy as np
# ----------------------------------------------------------------------
# PUNTO 1: AGREGACIÓN CON PYSPARK

# Cargar el DataFrame original
DataframeSpark = spark.read.table("Bronze.minciencia")

# 1. Preparar la columna de tiempo (Extrae solo la parte de la fecha)
df_preparado = DataframeSpark.select(
    F.col("time").cast("date").alias("fecha_dia"),
    F.col("ff_Valor").cast("float").alias("ff_Valor"),
    "CodigoNacional"
)

# 2. Calcular el promedio diario de ff_Valor por estación (DEFINE LA VARIABLE)
df_promedio_diario = df_preparado.groupBy("fecha_dia", "CodigoNacional").agg(
    F.avg("ff_Valor").alias("ff_Valor_PromedioDiario")
)

# 3. Ordenar y mostrar
df_promedio_diario = df_promedio_diario.orderBy("CodigoNacional", "fecha_dia")
df_promedio_diario.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


import pandas as pd
import pmdarima as pm
from datetime import timedelta
import numpy as np

# A. FILTRAR Y COLECTAR LA SERIE DE TIEMPO DESDE SPARK A PANDAS
# **NOTA:** Esta variable (df_promedio_diario) debe provenir del Punto 1
# y ya tiene la agregación diaria.
CODIGO_ESTACION = 180005.0 
df_serie_temporal_diaria = df_promedio_diario.filter(F.col("CodigoNacional") == CODIGO_ESTACION)

# Convertir el DataFrame de Spark a Pandas para usar pmdarima
df_pandas = df_serie_temporal_diaria.select("fecha_dia", "ff_Valor_PromedioDiario").toPandas()
df_pandas = df_pandas.set_index('fecha_dia') # La fecha es el índice de la serie

# B. EJECUTAR AUTO-ARIMA para encontrar los mejores parámetros
modelo_arima = pm.auto_arima(
    df_pandas['ff_Valor_PromedioDiario'].dropna(), 
    seasonal=False,                  
    suppress_warnings=True,
    stepwise=True,
    max_order=10
)

# C. CALCULAR LA PREDICCIÓN PARA 2 AÑOS
PASOS_PREDICCION = int(2 * 365.25) 

# D. REALIZAR LA PREDICCIÓN
prediccion_arima = modelo_arima.predict(n_periods=PASOS_PREDICCION)

# E. CREAR UN DATAFRAME DE RESULTADOS FINALES
ultima_fecha = df_pandas.index[-1]
# Generar el índice de fechas futuras (frecuencia Diaria 'D')
fechas_futuras = pd.date_range(
    start=ultima_fecha + timedelta(days=1),
    periods=PASOS_PREDICCION,
    freq='D' 
)

df_prediccion = pd.DataFrame({
    'fecha': fechas_futuras,
    'ff_Valor_Predicho': prediccion_arima.values,
    'CodigoNacional': CODIGO_ESTACION 
})

display(df_prediccion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler

# Cargar el DataFrame original
DataframeSpark = spark.read.table("Bronze.minciencia")

# 1. Limpieza y Creación de la Columna 'fecha_dia'
df_base = DataframeSpark.select(
    F.col("time").cast("timestamp").alias("fecha_hora"),
    F.col("ff_Valor").cast("float").alias("label"), # Renombrar la variable objetivo a 'label' (requerido por MLlib)
    F.col("CodigoNacional").cast("integer").alias("CodigoEstacion")
)

# 2. Filtrar por una Estación (necesario para el modelo inicial)
CODIGO_ESTACION = 180005 
df_estacion = df_base.filter(F.col("CodigoEstacion") == CODIGO_ESTACION)

# 3. Creación de Features Temporales y Agregación Diaria
df_features = df_estacion.withColumn("fecha_dia", F.col("fecha_hora").cast("date")) \
                         .withColumn("hora", F.hour(F.col("fecha_hora")))

# Calcular el promedio diario de 'label' (ff_Valor)
df_agregado = df_features.groupBy("fecha_dia", "CodigoEstacion").agg(
    F.avg("label").alias("label")
)

# 4. Creación de Features de Tiempo para el Modelo (para cada día)
df_final = df_agregado.withColumn("dia_del_anio", F.dayofyear(F.col("fecha_dia"))) \
                      .withColumn("dia_de_la_semana", F.dayofweek(F.col("fecha_dia"))) \
                      .withColumn("mes", F.month(F.col("fecha_dia"))) \
                      .withColumn("anio", F.year(F.col("fecha_dia"))) \
                      .orderBy("fecha_dia")

# 5. Ensamblar las Features en un vector (Requerido por MLlib)
feature_cols = ["dia_del_anio", "dia_de_la_semana", "mes", "anio"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# DataFrame final listo para el modelado
df_ml = assembler.transform(df_final).select("features", "label", "fecha_dia")

df_ml.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.regression import GBTRegressor
from pyspark.sql.window import Window

# 1. Separar datos de entrenamiento (ej. todo menos el último mes)
# En series temporales, no se usa un split aleatorio, sino temporal.

# Determinar la fecha de corte para el entrenamiento (Ejemplo: Último día - 30 días)
# Esto es complejo en PySpark sin un UDF, usaremos un enfoque simple:
df_train = df_ml.limit(int(df_ml.count() * 0.95))

# 2. Inicializar y Entrenar el Modelo GBT
gbt = GBTRegressor(labelCol="label", featuresCol="features", maxIter=10)
modelo = gbt.fit(df_train)

print("Modelo GBT entrenado con éxito.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
