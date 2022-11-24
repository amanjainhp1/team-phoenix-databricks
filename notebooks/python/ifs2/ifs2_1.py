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
supplies_xref = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM mdm.supplies_xref") \
    .load()
decay = read_redshift_to_df(configs) \
    .option("query", f"SELECT * FROM prod.decay") \
    .load()

# COMMAND ----------

tables = [
  ['prod.usage_share',usage_share],
  ['mdm.hardware_xref',hardware_xref],
  ['mdm.supplies_xref',supplies_xref],
  ['prod.decay',decay]
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

query = '''select cal_date
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , case when measure = 'COLOR_USAGE' then sum(units)/3
                        else sum(units)
                    end as units
                , ib_version
                , us.version
               from usage_share us
               inner join hardware_xref hw on us.platform_subset = hw.platform_subset
               where 1=1
                and hw.technology in ('INK', 'PWA')
                and us.measure in ('COLOR_USAGE' , 'K_USAGE' , 'HP_SHARE')
               group by
                  cal_date
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , ib_version
                , us.version
                '''
ink_usage_share = spark.sql(query)
ink_usage_share.createOrReplaceTempView("ink_usage_share")

query = '''select cal_date
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , sum(units) as units
                , ib_version
                , us.version
               from usage_share us
               inner join hardware_xref hw on us.platform_subset = hw.platform_subset
               where 1=1
                and hw.technology in ('LASER')
                and us.measure in ('COLOR_USAGE' , 'K_USAGE' , 'HP_SHARE')
               group by
                  cal_date
                , geography_grain
                , us.platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , ib_version
                , us.version
                '''
toner_usage_share = spark.sql(query)
toner_usage_share.createOrReplaceTempView("toner_usage_share")

# COMMAND ----------

query = '''select cal_date
                , geography_grain
                , platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , units
                , SUM(units) over 
                	(partition by 
                		geography_grain
                		,platform_subset
                		,customer_engagement
                		,hw_product_family
                		,measure
                		,ib_version
                		,version 
                	order by cal_date rows between unbounded preceding and current row)
                	as sum_of_usage_till_date
                ,ib_version
                ,version
                from ink_usage_share
                '''
ink_usage_share_sum_till_date = spark.sql(query)
ink_usage_share_sum_till_date.createOrReplaceTempView("ink_usage_share_sum_till_date")

# COMMAND ----------

ink_usage_share_sum_till_date.display()

# COMMAND ----------

## to add starting cal_date before doing sum_till_date
query = '''select cal_date
                , geography_grain
                , platform_subset
                , customer_engagement
                , hw_product_family
                , measure
                , units
                , SUM(units) over 
                	(partition by 
                		geography_grain
                		,platform_subset
                		,customer_engagement
                		,hw_product_family
                		,measure
                		,ib_version
                		,version 
                	order by cal_date rows between unbounded preceding and current row)
                	as sum_of_usage_till_date
                ,ib_version
                ,version
                from toner_usage_share
                '''
toner_usage_share_sum_till_date = spark.sql(query)
toner_usage_share_sum_till_date.createOrReplaceTempView("toner_usage_share_sum_till_date")
