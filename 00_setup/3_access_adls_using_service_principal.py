# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using service principal ###
# MAGIC 1. Register azure AD application/ service principal
# MAGIC 2. Generate a secret/password for application
# MAGIC 3. Set spark config with app/client id, Directory/Tenanat Id & secret
# MAGIC 4. Assign role 'storage blob data contirbutor' to the data lake
# MAGIC

# COMMAND ----------

#step 1
client_id = "64b52c26-33b9-454d-954a-ab8ab93938aa"
tanent_id = "82824541-b1a4-487b-b63a-e3ba44859984"

#step 2
client_secret = "fhk8Q~MJVruXps_h1-bNV3Y3OKMmYHtqKoAu~c1s"

# COMMAND ----------

#step 3
spark.conf.set("fs.azure.account.auth.type.formula1datalake5.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1datalake5.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1datalake5.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1datalake5.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1datalake5.dfs.core.windows.net", f"https://login.microsoftonline.com/{tanent_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net")

# COMMAND ----------

# better display
display(dbutils.fs.ls("abfss://demo@formula1datalake5.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake5.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

