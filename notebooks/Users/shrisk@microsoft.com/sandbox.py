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
state_df = smallradio.selectExpr("right(location,  2) as state", "length")
aliased_df = state_df.groupby("state").avg("length").withColumnRenamed("avg(length)", "AverageSongLength")
display(aliased_df)

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

#lets write this really interesting data back to an avro file
# Remove the file if it exists
myfilenamepath = "/myoutput/aggregated_data.avro"  
dbutils.fs.rm(myfilenamepath, True)

aliased_df.write.format("com.databricks.spark.avro").save(myfilenamepath)

# COMMAND ----------

#to get the aggregated data from avro file
avrodf = spark.read.format("com.databricks.spark.avro").load(myfilenamepath)
display(avrodf)

# COMMAND ----------

spark.sql("drop table if exists AggregatedSongData")

#save the aggregated avro file as a global table
avrodf.write.saveAsTable("AggregatedSongData") 

# COMMAND ----------

