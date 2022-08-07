# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sunkamsecret")

# COMMAND ----------

storage_account_name = "sunkamstorage"
storage_account_access_key = "blobsourcekeyvault"
file_location = "wasbs://sunkamstorage.blob.core.windows.net/"
file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

dbutils.secrets.list("sunkamSecreteScope")
#sunkamSecreteScope  # blobsourcekey

# COMMAND ----------

storage_account_name = "sunkamstorage"
storage_account_key = "blobsourcekeyvault"
container = "input"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://input@sunkamstorage.blob.core.windows.net/",
  mount_point="/mnt", 
  extra_configs = dbutils.secrets.get(scope = "sunkamsecret", key = "blobsourcekeyvault"))

# COMMAND ----------

dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
 mount_point = "/mnt/sunkamblob",
 extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
)

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

configs = {"fs.azure.account.key.YOUR_STORAGE.blob.core.windows.net" : "YOUR_KEY"}
 
dbutils.fs.mount(
  source = "wasbs://YOUR_CONTAINER@YOUR_STORAGE.blob.core.windows.net",
  mount_point = "/mnt/YOUR_FOLDER",
  extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/input/oauth2/token" ,"fs.azure.account.key.sunkamstorage.dfs.core.windows.net" : "blobsourcekey"}
dbutils.fs.mount(
  source = "abfss://inpt@sunkamstorage.dfs.core.windows.net",
  mount_point = "/dbfs/mnt/input",
  extra_configs = configs)

# COMMAND ----------


