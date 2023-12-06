# Databricks notebook source
raw_folder_path = "/mnt/formula1datalake5/raw"
processed_folder_path = "/mnt/formula1datalake5/processed"
presentation_folder_path = "/mnt/formula1datalake5/presentation"

# COMMAND ----------

### if dont have access to azure active directory
### use the abfss protocole
#raw_folder_path = "abfss://raw@formula1datalake5.dfs.core.windows.net"
#processed_folder_path = "abfss://processed@formula1datalake5.dfs.core.windows.net"
#presentation_folder_path = "abfss://presentation@formula1datalake5.dfs.core.windows.net"

# COMMAND ----------

