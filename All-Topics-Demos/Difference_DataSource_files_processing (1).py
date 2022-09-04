# Databricks notebook source
src_path ="dbfs:/FileStore/SampleFiles/csvfiles/demo_pipe.csv"


# COMMAND ----------

dbutils.fs.ls(src_path)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

# COMMAND ----------

schema = StructType([
      StructField("eid",IntegerType(),True),
      StructField("name",StringType(),True),
      StructField("salary",DoubleType(),True),
     StructField("place",StringType(),True)
])

# COMMAND ----------

# reading csv file which has pipe delimiter 
df_with_schema = spark.read.format("csv") \
       .option("delimiter",'|')\
      .schema(schema) \
      .load(src_path)

# COMMAND ----------

df_with_schema.display()

# COMMAND ----------

# dataframe to csv file with delimiter
df_with_schema.write.options(header='True', delimiter=',') \
 .csv("dbfs:/FileStore/SampleFiles/csvfiles/sample")


# COMMAND ----------


