# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using service principal ###
# MAGIC 1. get client_id, tanent_id, client_secret form key vault
# MAGIC 2. set spark config with app/client id, direcotyr/ tanenet id & secret
# MAGIC 3. call file system utilty to mount the storage
# MAGIC 4. explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

#step 1
client_id = dbutils.secrets.get(scope='formula1-scope', key='formual1-app-client-id')
tanent_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenent-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tanent_id}/oauth2/token"}


# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formula1datalake5.dfs.core.windows.net/",
  mount_point = "/mnt/formula1datalake5/demo",
  extra_configs = configs)

# COMMAND ----------

# better display
display(dbutils.fs.ls("/mnt/formula1datalake5/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1datalake5/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1datalake/demo/') # to unmount the data

# COMMAND ----------

display(dbutils.fs.mounts())