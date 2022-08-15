# Databricks notebook source
dbutils.secrets.list("testScope")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storagestggen2.dfs.core.windows.net",
    dbutils.secrets.get(scope="testScope", key="sourcekey"))

# COMMAND ----------

df= spark.read.csv("abfss://input@storagestggen2.dfs.core.windows.net/emp/*.csv",header=True)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.destgen2.dfs.core.windows.net",
    dbutils.secrets.get(scope="testScope", key="destkey"))

# COMMAND ----------

df.write.csv("abfss://empdest@destgen2.dfs.core.windows.net/sab/")
