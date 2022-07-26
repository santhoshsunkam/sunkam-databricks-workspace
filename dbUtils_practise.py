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


