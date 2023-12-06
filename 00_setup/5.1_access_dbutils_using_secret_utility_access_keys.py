# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using access keys ###
# MAGIC 1. set the spark fs.azure.account.key
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope='formula1-scope', key='formula1-dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1datalake5.dfs.core.windows.net", 
    formula1_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net")

# COMMAND ----------

# better display
display(dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake5.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

