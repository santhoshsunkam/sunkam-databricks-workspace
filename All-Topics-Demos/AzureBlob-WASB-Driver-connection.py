# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sunkamsecret")

# COMMAND ----------

storage_account_name = "sunkamstorage"
storage_account_access_key = "blobkvs2"

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

#config={"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
storage_account_name ="blobsinkstorage"
storage_account_access_key ="blobsinkkey"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_access_key)

# COMMAND ----------

#config={"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
storage_account_name ="blobsinkstorage"
container ="output2"
scope_name="sunkamsecret"
secret_name ="blobsinkkey"

config = {"fs.azure.account.key.blobsinkstorage.blob.core.windows.net":
      dbutils.secrets.get(scope_name, secret_name)}
dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
 mount_point = "/mnt/output2",
 extra_configs = config
)

# COMMAND ----------

target_file_path="/mnt/output2/*"
df.write.format("csv").option("header",True).option("inferSchema",True).load(target_file_path)

# COMMAND ----------

df.write.format("csv").save("/mnt/output2/datacsv")


# COMMAND ----------

display(df)

# COMMAND ----------


