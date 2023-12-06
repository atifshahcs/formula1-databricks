# Databricks notebook source
# MAGIC %md
# MAGIC ##### ingest drivers
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 = Read the JSON file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DataType, IntegerType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), True),
                                 StructField("driverRef", StringType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("code", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("nationality", StringType(), True),
                                 StructField("url", StringType(), True)
                                 ])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json("/mnt/formula1datalake5/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Rename columns and add new column
# MAGIC 1. driverid renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation for forename and surename

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_off_df = drivers_df.withColumnRenamed("driverid", "driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname") ) )

# COMMAND ----------

display(drivers_with_columns_off_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surename
# MAGIC 4. url
# MAGIC

# COMMAND ----------

driver_final_df = drivers_with_columns_off_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 write the file

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake5/processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1datalake5/processed/drivers"))

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalake5/processed/drivers

# COMMAND ----------

