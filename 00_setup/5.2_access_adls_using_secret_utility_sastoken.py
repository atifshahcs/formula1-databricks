# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using SAS Token ###
# MAGIC 1. set the spark config for SAS token
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

secret_vault_sas_token = dbutils.secrets.get(scope='formula1-scope', key='formula1-sas-key-05')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1datalake5.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1datalake5.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1datalake5.dfs.core.windows.net", secret_vault_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net")

# COMMAND ----------

# better display
display(dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake5.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

