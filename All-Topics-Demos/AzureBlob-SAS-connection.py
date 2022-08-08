# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.sunkamstorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sunkamstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sunkamstorage.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-08-08T01:47:13Z&st=2022-08-07T17:47:13Z&spr=https,http&sig=hD%2BhSZVEVuI04xW9oXG7RS8OiruHN4%2BH4tE4tXP4x9g%3D")

# COMMAND ----------

dbutils.secrets.list("sunkamsecret")

# COMMAND ----------

df = spark.read.csv('abfs://input@sunkamstorage.dfs.core.windows.net/input/netflix_titles (1).csv',header=True)

# COMMAND ----------

version()

# COMMAND ----------


