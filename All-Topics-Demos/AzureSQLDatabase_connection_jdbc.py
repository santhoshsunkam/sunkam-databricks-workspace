# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("SunkamScope")

# COMMAND ----------

dbutils.secrets.get()


# COMMAND ----------

# MAGIC %md
# MAGIC <h4> reading from sql database in azure <br> </h4>
# MAGIC <a>https://stackoverflow.com/questions/30983982/how-to-use-jdbc-source-to-write-and-read-data-in-pyspark</a>

# COMMAND ----------

jdbcUrl = dbutils.secrets.get("SunkamScope","sqldbjdbcurl")
connectionProperties = {
  "user" : dbutils.secrets.get("SunkamScope","sqlusername"),
  "password" : dbutils.secrets.get("SunkamScope","sqlpwd"),
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

#pushdown_query = "(select * from dbo.emp) emp_alias"
df = spark.read.jdbc(url=jdbcUrl, table="(select * from dbo.emp where salary<6000) emp1", properties=connectionProperties)



# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

jdbcUrl1 = dbutils.secrets.get("SunkamScope","sqldestjdbcurl")
connectionProperties = {
  "user" : dbutils.secrets.get("SunkamScope","sqlusername"),
  "password" : dbutils.secrets.get("SunkamScope","sqlpwd"),
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

df.write \
    .jdbc(jdbcUrl1, "dbo.destemo1",mode="append",
          properties=connectionProperties)


# COMMAND ----------


