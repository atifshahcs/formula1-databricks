# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ingest laptime csv
# MAGIC - this is the multiple csv file,

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 read file
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lapt_time_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 2 rename columns and add new columns

# COMMAND ----------

lap_time_df = spark.read\
.schema(lapt_time_schema)\
.csv("/mnt/formula1datalake5/raw/lap_times")

# COMMAND ----------

display(lap_time_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_time_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id")\
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 write file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1datalake5/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1datalake5/processed/lap_times"))

# COMMAND ----------

