# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake scoped credentials###
# MAGIC 1.   Copy the the strings into the Cluster advance option -> spark config
# MAGIC
# MAGIC     - fs.azure.account.key.formula1datalake5.dfs.core.windows.net
# MAGIC     - cMT7netlGGiHdMhxIhK2RwDilSbX0vQ2KOQH9WHNRvbMhoCZpsEkmXK8cSBKZXWV8W9BjBjr5clZ+ASt2tWW3w==
# MAGIC     - update and restart the cluster,
# MAGIC     - Now we dont need to set the spark.conf.set(), as they are already asigned to the cluster
# MAGIC     - All the notebook connected to this cluster will have access to the storage and data.
# MAGIC         -Not a good practice and should avoid.
# MAGIC         
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

# Copy the the strings into the Cluster advance option -> spark config
    # fs.azure.account.key.formula1datalake5.dfs.core.windows.net
    # cMT7netlGGiHdMhxIhK2RwDilSbX0vQ2KOQH9WHNRvbMhoCZpsEkmXK8cSBKZXWV8W9BjBjr5clZ+ASt2tWW3w==
    # update and restart the cluster,
    # Now we dont need to set the spark.conf.set(), as they are already asigned to the cluster
    # All the notebook connected to this cluster will have access to the storage and data.
        # Not a good practice and should avoid.
        
#spark.conf.set(
#    "fs.azure.account.key.formula1datalake5.dfs.core.windows.net", 
#    "cMT7netlGGiHdMhxIhK2RwDilSbX0vQ2KOQH9WHNRvbMhoCZpsEkmXK8cSBKZXWV8W9BjBjr5clZ+ASt2tWW3w=="
#)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net")

# COMMAND ----------

# better display
display(dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake5.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

