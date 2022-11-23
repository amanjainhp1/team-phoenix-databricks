# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

dbutils.widgets.text("usage_share_version",'') # set usage_share_version to mark as official

# COMMAND ----------

usage_share_version = dbutils.widgets.get("usage_share_version")
if usage_share_version == "":
    usage_share_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM prod.usage_share") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]

# COMMAND ----------

usage_share_version

# COMMAND ----------

usage_share = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.usage_share WHERE version = '{usage_share_version}'") \
    .load()
hardware_xref = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.hardware_xref") \
    .load()

# COMMAND ----------

tables = [
  ['prod.usage_share',usage_share]
]

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]
    print(f'loading {table[0]}...')
    # Write the data to its target.
    df.write \
      .format(write_format) \
      .mode("overwrite") \
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

query = '''select record
                , cal_date
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , measure
                , units
                , ib_version
                , source
                , us.version
                , us.load_date
               from usage_share us
               inner join hardware_xref hw on us.platform_subset = hw.platform_subset
               where 1=1
                and hw.technology in ('INK', 'PWA')
                and us.measure in ('COLOR_USAGE' , 'K_USAGE' , 'HP_SHARE')
                '''
ink_usage_share = spark.sql(query)
ink_usage_share.createOrReplaceTempView("ink_usage_share")

query = '''select record
                , cal_date
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , measure
                , units
                , ib_version
                , source
                , us.version
                , us.load_date
               from usage_share us
               inner join hardware_xref hw on us.platform_subset = hw.platform_subset
               where 1=1
                and hw.technology in ('LASER')
                and us.measure in ('COLOR_USAGE' , 'K_USAGE' , 'HP_SHARE')
                '''
toner_usage_share = spark.sql(query)
toner_usage_share.createOrReplaceTempView("toner_usage_share")
