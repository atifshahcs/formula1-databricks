# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("2_0_ingest_circuits_file",0, {"p_data_source": "Ergast API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

