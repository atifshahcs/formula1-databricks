# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")\
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id < 70 ")\
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left join

# COMMAND ----------

# left outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)


# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right join

# COMMAND ----------

# Right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full join

# COMMAND ----------

# full outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi join

# COMMAND ----------

# semi  outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross joins

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

