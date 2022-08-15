# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.storagestggen2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storagestggen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storagestggen2.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-08-21T21:51:09Z&st=2022-08-15T13:51:09Z&spr=https&sig=xutrm6CqBUoiKY1dr9%2F1t5YtylJTqifxcfj2b2mWY%2Fs%3D")

# COMMAND ----------

df = spark.read.csv("abfss://input@storagestggen2.dfs.core.windows.net/emp/*",header=True,inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(10)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.destgen2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.destgen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.destgen2.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-08-18T22:04:53Z&st=2022-08-15T14:04:53Z&spr=https&sig=Du1FOBm84kVGbLDuANtCas8Qwvyl0%2BW%2B2qOYuh6z6VE%3D")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.csv("abfss://empdest@destgen2.dfs.core.windows.net/sad/",header=True)

# COMMAND ----------


