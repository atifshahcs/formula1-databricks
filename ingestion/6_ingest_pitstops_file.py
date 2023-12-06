# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest pitstop json file
# MAGIC - this is the multi line json file,
# MAGIC - spark by default read single like json file
# MAGIC - for multiline we need to tell the spark

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 read file
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("stop",StringType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("duration",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 2 rename columns and add new columns

# COMMAND ----------

pit_stops_df = spark.read\
.schema(pit_stops_schema)\
.option("multiLine",True)\
.json("/mnt/formula1datalake5/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id")\
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 write file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1datalake5/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1datalake5/processed/pit_stops"))

# COMMAND ----------

