# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sunkamsecret")

# COMMAND ----------

storage_account_name = "sunkamstorage"
storage_account_access_key = "blobkvs"
file_location = "wasbs://sunkamstorage.blob.core.windows.net/"
file_type = "csv"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_access_key)

# COMMAND ----------

#config={"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
storage_account_name ="sunkamstorage"
container ="input"
scope_name="sunkamsecret"
secret_name ="blobkvs2"

config = {"fs.azure.account.key.sunkamstorage.blob.core.windows.net":
      dbutils.secrets.get(scope_name, secret_name)}
dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
 mount_point = "/mnt/input",
 extra_configs = config
)

# COMMAND ----------

src_file_path="/mnt/input/*"
df= spark.read.csv(src_file_path,header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.head()

# COMMAND ----------

dest_file_path="/output/"
df.write.format("csv").option("header",True).save(dest_file_path)

# COMMAND ----------


