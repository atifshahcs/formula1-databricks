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
client_id = dbutils.secrets.get(scope='formula1-scope', key='formual1-app-client-id')
tanent_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenent-id')

#step 2
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

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

