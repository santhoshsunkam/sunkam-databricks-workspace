# Databricks notebook source
# sunkamsecrets  
# to create secretes scrope - https://adb-<instance>.10.azuredatabricks.net/#secrets/createScopes
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sunkamScope")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.blobsourcestg.dfs.core.windows.net",
    dbutils.secrets.get(scope="sunkamScope", key="blobsource"))

# COMMAND ----------

src_file_path="abfss://input@blobsourcestg.dfs.core.windows.net/*.csv"

# COMMAND ----------

df2 = spark.read.format("csv").option("header",True).load(src_file_path)

# COMMAND ----------


df.printSchema()

# COMMAND ----------

display(df2)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.blobsinkstg.dfs.core.windows.net",
    dbutils.secrets.get(scope="sunkamScope", key="blobsink"))

# COMMAND ----------

target_file_path = "abfss://output@blobsinkstg.dfs.core.windows.net"

# COMMAND ----------

df2.coalesce(1).write.option("header",True).csv("abfss://output@blobsinkstg.dfs.core.windows.net/santh")

# COMMAND ----------



# COMMAND ----------


