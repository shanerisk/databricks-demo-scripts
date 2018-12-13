# Databricks notebook source
import sys
import functools
import warnings

if sys.version < "3":
    from itertools import imap as map

if sys.version >= '3':
    basestring = str

from pyspark import since, SparkContext
from pyspark.rdd import ignore_unicode_prefix, PythonEvalType
from pyspark.sql.column import Column, _to_java_column, _to_seq, _create_column_from_literal
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, DataType

# COMMAND ----------

print("Hello World")

# COMMAND ----------

#retrieve the sample data and display it as a table
smallradio = spark.sql("Select * From small_radio_json_json")

display(smallradio.select("*"))

# COMMAND ----------

#display(smallradio.select(pyspark.sql.functions.substring("location", pyspark.sql.functions.length("location")-1, 2)).alias("l"))
statecoldf = smallradio.selectExpr("right(location,  2)").alias("state")
display(statecoldf)

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

