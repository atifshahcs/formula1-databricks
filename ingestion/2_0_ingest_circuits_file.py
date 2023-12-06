# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the CSV file using spark dataframe reader #####

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

print(raw_folder_path)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(), False),
                                     StructField('circuitRef', StringType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('location', StringType(), True),
                                     StructField('country', StringType(), True),
                                     StructField('lat', DoubleType(), True),
                                     StructField('lng', DoubleType(), True),
                                     StructField('alt', IntegerType(), True),
                                     StructField('url', StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.option('header', True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/circuits.csv', header=True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Select the required columns ###
# MAGIC

# COMMAND ----------

# one way to select the column
circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")


# COMMAND ----------

# another way to select the columns
circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

# another way to select the columns
circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

# another way to select the columns 
# prefer, its give flexibility to change names too of the columns. 
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 = Rename the columns ###

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source", lit(v_data_source))
    

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4: Add a new column  ###

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4: write the dataframe to parquet file ###

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalake5/processed/circuits
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: reading back the parquet file

# COMMAND ----------

# lets check and read the parquet file back

df = spark.read.parquet("/mnt/formula1datalake5/processed/circuits/")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("success!")