# Databricks notebook source
# sunkamsecrets  
# to create secretes scrope - https://adb-<instance>.10.azuredatabricks.net/#secrets/createScopes
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sunkamsecrets")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.sunkamstorage.dfs.core.windows.net",
    dbutils.secrets.get(scope="sunkamsecrets", key="blobsourcekey"))

# COMMAND ----------

src_file_path="abfss://input@sunkamstorage.dfs.core.windows.net/*.csv"

# COMMAND ----------

df = spark.read.format("csv").save(src_file_path).option(header,True).option(inferedSchema,True)


# COMMAND ----------

df2 = spark.read.option("header",True) \
     .csv(src_file_path)
display(df2)

# COMMAND ----------

spark.read.csv(src_file_path).limit(5)


# COMMAND ----------

df2.write.option("header",True) \
 .csv("/tmp/spark_output/zipcodes123")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.sunkamstorage.dfs.core.windows.net",
    dbutils.secrets.get(scope="sunkamsecrets", key="blobsinkkey2"))

# COMMAND ----------

df2.write.option("header",True).csv("abfss://output@sunkamstorage.dfs.core.windows.net")

# COMMAND ----------


