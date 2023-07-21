# Databricks notebook source
import spark

# COMMAND ----------

num_list = [1,2,3,4]
rdd =sc.parallelize(num_list)
squared = rdd.map(lambda x : x*x).collect()

# COMMAND ----------

for i in squared:
    print(i)

# COMMAND ----------

type(squared)

# COMMAND ----------

type(rdd)

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

working_forecast_country = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM prod.working_forecast_country WHERE version = (SELECT max(version) from prod.working_forecast_country)") \
    .load()

calendar = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM mdm.calendar") \
    .load()

# COMMAND ----------

calendar = calendar.withColumnRenamed("record","record1")

# COMMAND ----------

df = working_forecast_country.join(calendar,working_forecast_country.cal_date ==  calendar.date,"inner")

# COMMAND ----------

df.display()

# COMMAND ----------

working_forecast_y = df.select("record","geography_grain","geography","country","platform_subset","customer_engagement","cartridges","imp_corrected_cartridges","load_date","version","fiscal_yr")

# COMMAND ----------

working_forecast_y_less = working_forecast_y.filter(col("fiscal_yr")<2028)

# COMMAND ----------

temp_df = working_forecast_y_less.groupBy('fiscal_yr').sum('cartridges')

# COMMAND ----------

temp_df = temp_df.orderBy('fiscal_yr')

# COMMAND ----------

from pyspark.sql.types import IntegerType
temp_df = temp_df.withColumn('fiscal_yr',temp_df['fiscal_yr'].cast(IntegerType()))

# COMMAND ----------

temp_df.display()

# COMMAND ----------

from pyspark import SparkContext
sc =SparkContext()


rdd=sc.parallelize([1,2,3,4,5])
rddCollect = rdd.collect()
print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))
print(rddCollect)

# COMMAND ----------


