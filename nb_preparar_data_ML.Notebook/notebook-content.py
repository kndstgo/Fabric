# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cbc59747-ba88-43d7-9481-367e2c8c2252",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "23fc4e08-257c-44ed-9f11-927b7ec4610d",
# META       "known_lakehouses": [
# META         {
# META           "id": "cbc59747-ba88-43d7-9481-367e2c8c2252"
# META         },
# META         {
# META           "id": "3e5225db-aae3-44c7-a1e4-d744681cf6d8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Preparar data para ML analítica predictiva

df = spark.read.table("Silver.ft_tipo_cambio_dia")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM Silver.ft_tipo_cambio_dia A
# MAGIC INNER JOIN dim_monedas  B
# MAGIC ON B.currencycode = A.tocurrencycode

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Integrar SPARK con SQL para crear un DataFrame
df_entrenamiento = spark.sql(
"""

SELECT * 
FROM Silver.ft_tipo_cambio_dia A
INNER JOIN dim_monedas  B
ON B.currencycode = A.tocurrencycode




"""
)


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

# CELL ********************


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

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
