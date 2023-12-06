# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using access keys ###
# MAGIC 1. set the spark fs.azure.account.key
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1datalake5.dfs.core.windows.net", 
    "cMT7netlGGiHdMhxIhK2RwDilSbX0vQ2KOQH9WHNRvbMhoCZpsEkmXK8cSBKZXWV8W9BjBjr5clZ+ASt2tWW3w=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net")

# COMMAND ----------

# better display
display(dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake5.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

