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

# Ultimo dato en destino

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT max(time) as maximo
# MAGIC FROM Silver.minciencia_incremental_pl

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
