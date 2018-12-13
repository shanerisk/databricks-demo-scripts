# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

print("Hello World")

# COMMAND ----------

#retrieve the sample data and display it as a table
smallradio = spark.sql("Select * From small_radio_json_json")

display(smallradio.select("*"))

# COMMAND ----------

#display(smallradio.select(pyspark.sql.functions.substring("location", pyspark.sql.functions.length("location")-1, 2)).alias("l"))
state_df = smallradio.selectExpr("right(location,  2) as state", "length").alias("state")
agg_df = state_df.groupby("state").avg("length")
display(agg_df)

# COMMAND ----------

#now do the same thing as a sql statement
smallradio.registerTempTable("databricks_song_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select right(location, 2) as state, avg(length) as AverageSongLength
# MAGIC from databricks_song_table
# MAGIC Group by right(location, 2)

# COMMAND ----------

