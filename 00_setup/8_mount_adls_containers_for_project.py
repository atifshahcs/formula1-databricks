# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using service principal ###
# MAGIC 1. get client_id, tanent_id, client_secret form key vault
# MAGIC 2. set spark config with app/client id, direcotyr/ tanenet id & secret
# MAGIC 3. call file system utilty to mount the storage
# MAGIC 4. explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls(storage_acccount_name, container_name):
    #step 1 Get secrets from key vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='formual1-app-client-id')
    tanent_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenent-id')
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



# COMMAND ----------

# Mount raw container
mount_adls('formula1datalake5','raw')

# COMMAND ----------

# Mount processed container
mount_adls('formula1datalake5','processed')

# COMMAND ----------

# Mount presentation container
mount_adls('formula1datalake5','presentation')

# COMMAND ----------

#dbutils.fs.unmount('/mnt/formula1datalake/demo/') # to unmount the data

# COMMAND ----------

#display(dbutils.fs.mounts())