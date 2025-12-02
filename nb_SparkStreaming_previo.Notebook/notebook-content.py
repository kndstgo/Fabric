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

# Crear ficheros JSON con data aleatoria

import json
import os
import uuid
import random
import time
from datetime import datetime

#Configuracion fichero
carpeta_salida= "/lakehouse/default/Files/ficheros_json"
num_ficheros= 15
seg_espera= 7

#Verificacion de existencia de carpetas
os.makedirs(carpeta_salida, exist_ok= True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(num_ficheros):

    #Variables generales
    now= datetime.utcnow()
    timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")

    #simulacion de mensaje del sensor
    registro = {
        "id": str(uuid.uuid4()),
        "temp": round(random.uniform(21.0, 35.0),2),
        "timestamp": now.isoformat()
    }

    #construir el fichero json
    filename = f"temp_{timestamp_str}.json"
    filepath = os.path.join(carpeta_salida, filename)

    #materializar el fichero JSON
    with open(filepath, "w") as f:
        json.dump(registro, f)
    print(f"Ok - [{i+1}/{num_ficheros}] Escribió: {filename}")
    time.sleep(seg_espera)

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
