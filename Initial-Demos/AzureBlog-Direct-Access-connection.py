# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sunkamSecreteScope")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.sunkamstorage.blob.core.windows.net",
    dbutils.secrets.get(scope="sunkamSecreteScope", key="blobsourcekey"))

# COMMAND ----------

src_file_path="wasbs://sunkamstorage.blob.core.windows.net/input/netflix_titles (1).csv"

# COMMAND ----------


#df = spark.read.csv("abfss://input@sunkamstorage.dfs.core.windows.net/input/*.csv")

diamonds = spark.read.format('csv').options(header='true', inferSchema='true').load('src_file_path')


# COMMAND ----------


df = spark.read.format("csv")
                  .load("/tmp/resources/zipcodes.csv")
//       or
df = spark.read.format("org.apache.spark.sql.csv")
                  .load("/tmp/resources/zipcodes.csv")
df.printSchema()


# COMMAND ----------


