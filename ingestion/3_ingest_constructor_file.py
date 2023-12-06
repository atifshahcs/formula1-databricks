# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructor.json file#####

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 Read the json the using the spark dataframe reader #####
# MAGIC

# COMMAND ----------

constructers_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructers_df = spark.read \
.schema(constructers_schema) \
.json("/mnt/formula1datalake5/raw/constructors.json")

# COMMAND ----------

constructers_df.printSchema()

# COMMAND ----------

display(constructers_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 2 Drop unwanted columns #####

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructers_drop_df = constructers_df.drop(col("url"))

# COMMAND ----------

display(constructers_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 Rename columns and add ingestion date #####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructers_final_df = constructers_drop_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constrcutor_ref") \
                                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructers_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 write output to parquet file

# COMMAND ----------

constructers_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake5/processed/constructor")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalake5/processed/constructor

# COMMAND ----------

