# Databricks notebook source
df =spark.read.option("multiline","true").json("dbfs:/FileStore/SampleFiles/jsonfiles/new_265.txt")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#[batters, name, type, id, ppu, topping];
#eDF.select(explode(eDF.intlist).alias("anInt")).collect()
df2= df.withColumn("topping1",explode(col("topping")))\
        .drop(col("topping"))

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.select("topping1").show()

# COMMAND ----------

df3= df2.select("*",col("topping1.id").alias("style_id"),col("topping1.type").alias("style_nm")).drop("topping1")

# COMMAND ----------

display(df3)

# COMMAND ----------

df_st = spark.read.option("multiline","true").json("dbfs:/FileStore/SampleFiles/jsonfiles/Students.json")

# COMMAND ----------

display(df_st)

# COMMAND ----------

df_st1= df_st.select(explode(col("marks")).alias("marks1"),"*").drop(col("marks"))

# COMMAND ----------

display(df_st1)

# COMMAND ----------

df_st1.printSchema()

# COMMAND ----------

df_st2= df_st1.select("marks1.*","*").drop("marks1")

# COMMAND ----------

display(df_st2)

# COMMAND ----------

df_final = df_st2.select("sid","sname","city","email","hallticket","result")

# COMMAND ----------

display(df_final)
