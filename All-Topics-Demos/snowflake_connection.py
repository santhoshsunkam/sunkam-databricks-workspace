# Databricks notebook source
user = dbutils.secrets.get(scope="sunkamScope", key="sfuser")
password = dbutils.secrets.get(scope="sunkamScope", key="sfpassword")
sfUrl = dbutils.secrets.get(scope="sunkamScope", key="sfUrl")

# snowflake connection options
options = {
  "sfUrl": sfUrl,
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "SANTHOSHTEST",
  "sfSchema": "HR",
  "sfWarehouse": "SANTHWAREHOUSE"
}


# COMMAND ----------

#sunkamScope 
df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "emp") \
  .load()
 
display(df)

# COMMAND ----------

# Generate a simple dataset containing five values and write the dataset to Snowflake.
df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "t_emp") \
  .save()
