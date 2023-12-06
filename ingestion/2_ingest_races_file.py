# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest race file ###

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - read the csv file using the spark dataframe reader API ###

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType (fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

races_df = spark.read\
    .option ("header",True) \
    .schema(races_schema) \
    .csv("/mnt/formula1datalake5/raw/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-d HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - select only the column required ###

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("name"), col("circuitId").alias("circuit_id"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 -- write the output to parquet file

# COMMAND ----------

races_selected_df.write.mode("overwrite").parquet("/mnt/formula1datalake5/processed/races")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write the output to processed container in parquet format ###

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1datalake5/processed/races")

# COMMAND ----------

# read it back
display(spark.read.parquet("/mnt/formula1datalake5/processed/races"))

# COMMAND ----------

