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
# META         },
# META         {
# META           "id": "cbc59747-ba88-43d7-9481-367e2c8c2252"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Trabajamos las tablas sin tiempo complejo (materializar parquets en tablas)

# CELL ********************

# Leer los ficheros parquet que son los resultados de  la imporación anterrio

df_department = spark.read.parquet("Files/humanresources/department")
df_employeedh = spark.read.parquet("Files/humanresources/employeedepartmenthistory")
df_employeeph = spark.read.parquet("Files/humanresources/employeepayhistory")
df_jobcandidate = spark.read.parquet("Files/humanresources/jobcandidate")
# df_shift = spark.read.parquet("Files/humanresources/shift")

display(df_department)
df_employeedh
df_employeeph
df_jobcandidate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar los dataframes como tablas

# CELL ********************

# Guardar los dataframes como tablas

df_department.write.format("delta").mode("overwrite").saveAsTable("Bronze.hr_department")
df_employeedh.write.format("delta").mode("overwrite").saveAsTable("Bronze.hr_employeedepartmenthistory")
df_employeeph.write.format("delta").mode("overwrite").saveAsTable("Bronze.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("Bronze.hr_jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Trabajamos la tabla más compleja

# CELL ********************

import pandas as pd
from pyspark.sql import functions as F
#1) Lee y limpia
df_shift_pandas = pd.read_parquet("/lakehouse/default/Files/humanresources/shift")
df_shift_pandas = df_shift_pandas.drop(columns="modifieddate")

#2) Normaliza tipos: time to string
for c in ["starttime", "endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str)  # "HH:MM:SS"

#3) de pandas a Spark

df_shift = spark.createDataFrame(df_shift_pandas)

#4) Casts finales
df_shift = (df_shift
    .withColumn("shiftid", F.col("shiftid").cast("int"))
    .withColumn("name", F.col("name").cast("string"))
    .withColumn("starttime", F.col("starttime").cast("string"))
    .withColumn("endtime", F.col("endtime").cast("string"))
)

#5) Guardar como tabla delta en Bronze
df_shift.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Bronze.hr_shift")

# Verification
spark.sql("SELECT * FROM Bronze.hr_shift").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creacion de tablas de Hechos y Dimensiones para guardarlas en Silver

# CELL ********************

#Crear tabla de Hechos
df_ft_costos_rrhh = spark.sql(
""" 
SELECT
  edh.businessentityid,
  edh.departmentid,
  edh.shiftid,
  eph.ratechangedate,
  eph.rate,
  eph.payfrequency,
  CASE
    WHEN eph.payfrequency = 1 THEN eph.rate / 30   -- Mensual
    WHEN eph.payfrequency = 2 THEN eph.rate / 15   -- Quincenal
    ELSE eph.rate
  END AS costo_dia
FROM Bronze.hr_employeedepartmenthistory edh
LEFT JOIN Bronze.hr_employeepayhistory eph
  ON edh.businessentityid = eph.businessentityid
ORDER BY
  edh.businessentityid,
  eph.ratechangedate;
"""
)
display(df_ft_costos_rrhh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardar Tabla de Hechos

# CELL ********************

# Guardar Tabla de Hechos
df_ft_costos_rrhh.write.format("delta").mode("overwrite").saveAsTable("Silver.ft_costos_rrhh_dia_turno_dpto_business_entity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Crear Tabla Dimension Trabajadores

# CELL ********************

# Dimension Trabajadores
df_dim_trabajador = spark.sql(
""" 
SELECT
  businessentityid,
  MIN(startdate)  AS fechainicio,
  MAX(enddate) AS fechatermino
FROM Bronze.hr_employeedepartmenthistory
GROUP BY businessentityid  
"""
)

display(df_dim_trabajador)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardar tabla de Dimensiones

# CELL ********************

# Guardar Tabla de Dimensiones
df_dim_trabajador.write.format("delta").mode("overwrite").saveAsTable("Silver.dim_trabajador")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creamos Tabla Dimension Departamento

# CELL ********************

#Creamos la tabla de dimension departamento

df_dim_departamento = spark.sql(
"""
SELECT
  departmentid,
  name AS departmentname,
  groupname
FROM Bronze.hr_department;
"""
)

display(df_dim_departamento)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardamos la tabla Dimension Departamento

# CELL ********************

# Guardar Dimension Departamento

df_dim_departamento.write.format("delta").mode("overwrite").saveAsTable("Silver.dim_departamento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creamos Tabla Dimensión Turno

# CELL ********************

# Creamos Tabla Dim Turno
df_dim_turno = spark.sql(
""" 
SELECT
shiftid,
name AS shiftname,
starttime,
endtime
FROM Bronze.hr_shift;
"""
)
display(df_dim_turno)

df_dim_turno.write.mode("overwrite").saveAsTable("Silver.dim_turno")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
