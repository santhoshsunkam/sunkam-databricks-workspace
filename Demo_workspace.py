# Databricks notebook source
list1 = [(1,"santhosh",5000),(2,"naressh",4000)]   
type(list1)

# COMMAND ----------

print(list1)

# COMMAND ----------

## creating the rdd using parallelize method
rdd1 = sc.parallelize(list1)
type(rdd1)
rdd1.getNumPartitions()

# COMMAND ----------

# display the rdd1
# collect() is used to display the rdd, it takes lot of memory not recommended 

rdd1.collect()


# COMMAND ----------

rdd1.take(2)

# COMMAND ----------

# creating the Data Frame using exisitng RDD 

df = rdd1.toDF()

# COMMAND ----------

df.display()

# COMMAND ----------


