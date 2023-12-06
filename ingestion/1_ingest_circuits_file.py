# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1: Read the CSV file using spark dataframe reader #####

# COMMAND ----------


def mount_adls(storage_acccount_name, container_name):
    #step 1 Get secrets from key vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
    tanent_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')
    
    #Step 2 set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tanent_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_acccount_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_acccount_name}/{container_name}")

    #Step 3 mount storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_acccount_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_acccount_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

# if data is not mount, first mount the data using the following function
# mount raw container
mount_adls('formula1datalake5','raw')
# mount processed container
mount_adls('formula1datalake5','processed')
# mount presentation container
mount_adls('formula1datalake5','presentation')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalake5/raw

# COMMAND ----------

# For debug purposes
#dbutils.secrets.listScopes()
#dbutils.secrets.list(scope='formula1-scope')

#dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')


# COMMAND ----------

##### this file starting for here, the above are commands for debug #####
circuits_df = spark.read.option('header', True) \
    .csv('dbfs:/mnt/formula1datalake5/raw/circuits.csv', header=True)

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
    .csv('dbfs:/mnt/formula1datalake5/raw/circuits.csv', header=True)

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

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")
    

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4: Add a new column  ###

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4: write the dataframe to parquet file ###

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake5/processed/circuits")

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

