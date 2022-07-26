# Databricks notebook source
dbutils.help()

# COMMAND ----------

## Checking dbutils fs module 

dbutils.fs.help()

# COMMAND ----------

# Databricks root directory 
dbutils.fs.ls("dbfs:/")

# COMMAND ----------

# databricks provides sample files in this directory 
dbutils.fs.ls("dbfs:/databricks-datasets/")

# COMMAND ----------

# dbfs:/FileStore/
dbutils.fs.ls("dbfs:/FileStore/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# Copying from one loc to other loc here I am c
dbutils.fs.cp("databricks-datasets/amazon/README.md","dbfs:/FileStore/")

# COMMAND ----------

# making directory santhosh in FileStore, you can check in data folder sidebar
dbutils.fs.mkdirs("dbfs:/FileStore/Santhosh")

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd 
# MAGIC ls

# COMMAND ----------

dbutils.fs.put("dbfs:/FileStore/Santhosh/sample.txt","this is sample fiel written for the sample file using put coomand")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/Santhosh/sample.txt",30)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Santhosh/",True)

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/Santhosh/sample.txt",30)

# COMMAND ----------


