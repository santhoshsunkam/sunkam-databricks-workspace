# Databricks notebook source
print("santhosh")

# COMMAND ----------

print("Pyspark in Colab")

# COMMAND ----------

pip install pyspark

# COMMAND ----------

!python --version

# COMMAND ----------

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# COMMAND ----------

df = spark.read.format("csv").option("header",True).option("inferedSchema",True).load("/content/sample_data/Bosch Pro 1000W.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()


# COMMAND ----------


