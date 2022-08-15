# Databricks notebook source
# list secrets scope 
dbutils.secrets.list("testScope")

# COMMAND ----------

# set the configuation for ADLS gen2 storage source
spark.conf.set("fs.azure.account.auth.type.storagestggen2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storagestggen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storagestggen2.dfs.core.windows.net", "0a2729ce-0049-431d-8012-23f7d02c593a")
spark.conf.set("fs.azure.account.oauth2.client.secret.storagestggen2.dfs.core.windows.net", dbutils.secrets.get(scope="testScope",key="appsecret1"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storagestggen2.dfs.core.windows.net", "https://login.microsoftonline.com/8bc4a72a-aff1-4ed0-a126-531a958b770a/oauth2/token")

# COMMAND ----------

df = spark.read.csv("abfss://input@storagestggen2.dfs.core.windows.net/emp/*.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()
df.show(5)

# COMMAND ----------

# set the ADLS gen2 config with service principle
spark.conf.set("fs.azure.account.auth.type.destgen2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.destgen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.destgen2.dfs.core.windows.net", "0a2729ce-0049-431d-8012-23f7d02c593a")
spark.conf.set("fs.azure.account.oauth2.client.secret.destgen2.dfs.core.windows.net", dbutils.secrets.get(scope="testScope",key="appsecret1"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.destgen2.dfs.core.windows.net", "https://login.microsoftonline.com/8bc4a72a-aff1-4ed0-a126-531a958b770a/oauth2/token")

# COMMAND ----------

df.write.csv("abfss://empdest@destgen2.dfs.core.windows.net/santh/")

# COMMAND ----------

dbutils.fs.ls("abfss://empdest@destgen2.dfs.core.windows.net/santh/")

# COMMAND ----------


