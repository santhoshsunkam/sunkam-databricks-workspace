# Databricks notebook source
#dbutils help
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

#dbutils text widget widget name is x 
dbutils.widgets.text("x","","enter x value")

# COMMAND ----------

#dbutils text widget value return from textbox above 
dbutils.widgets.get("x")

# COMMAND ----------

# create a dict with country name and capital 
country_name = {"India":"New Delhi", "Japan":"Toyko", "Canada":"Ottava","United Kingdom": "London"}

# COMMAND ----------

#print dictionary 
print(country_name)

# COMMAND ----------

# display dict values using for loop 
for k, v in country_name.items():
    print(" value {} and {}".format(k,v))
    

# COMMAND ----------

# function to return country capita from dictionary 
def countrydetails(country):
    capital =country_name.get(country,"Invalid country")
    return capital

# COMMAND ----------

# text widget using function 
def countryWidget(country):
    dbutils.widgets.text("country","","Enter country name")
    x=dbutils.widgets.get("country")
    return x

# COMMAND ----------

# return country values from dictionary by taking country inout from widget 
x = countryWidget(x)
print("Input Country:"+x)
cap =countrydetails(x)
print("capital :"+cap)

# COMMAND ----------


