# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using SAS Token ###
# MAGIC 1. set the spark config for SAS token
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1datalake5.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1datalake5.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1datalake5.dfs.core.windows.net", "sp=rl&st=2023-11-24T21:26:32Z&se=2023-11-25T05:26:32Z&spr=https&sv=2022-11-02&sr=c&sig=39vdiqkkAOJ%2ByozaDq2CC8%2FJAN7gDPoyIBg9N4pLA6c%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net")

# COMMAND ----------

# better display
display(dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake5.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

