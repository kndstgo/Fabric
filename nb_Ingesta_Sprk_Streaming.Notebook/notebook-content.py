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

# Notebook de Ingesta

#Invocar librerias
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import json
import pyspark.sql.functions as f
import time 

#Configuracion
ruta_origen= "Files/ficheros_json"
nombre_tabla = "temperatura_simulada"
ruta_check = "Files/ficheros_checkpoint"

#Esquema del fichero de origen (data JSON de origen)
file_schema = StructType() \
.add("id", StringType()) \
.add("temp", DoubleType()) \
.add("timestamp", TimestampType())

spark.sql(f"CREATE TABLE IF NOT EXISTS {nombre_tabla}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer el fichero de origen con Spark
fichero_raiz_df = spark.readStream\
    .schema(file_schema)\
    .option("maxFilesPerTrigger", 1)\
    .json(ruta_origen)

# Agregar el timestamp de procesamiento
fichero_mod_df = fichero_raiz_df.withColumn("ts_proces", f.current_timestamp())

#Escribimos la data e la tabla delta
deltastream = fichero_mod_df\
    .writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("mergeSchema", True)\
    .option("checkpointLocation", ruta_check)\
    .start(f"Tables/{nombre_tabla}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze.temperatura_simulada")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fichero_raiz_df.isStreaming

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.isActive

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.lastProgress

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
