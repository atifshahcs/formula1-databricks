# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local view 

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(1)
# MAGIC From v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global view 

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from global_temp.gv_race_results
# MAGIC

# COMMAND ----------

