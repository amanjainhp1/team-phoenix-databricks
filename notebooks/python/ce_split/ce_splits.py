# Databricks notebook source
import json
from datetime import date
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

def get_data_by_table(table):
    df = read_redshift_to_df(configs) \
        .option("dbtable", table) \
        .load()
    
    for column in df.dtypes:
        if column[1] == 'string':
            df = df.withColumn(column[0], f.upper(f.col(column[0]))) 

    return df

# COMMAND ----------

max_version_info = call_redshift_addversion_sproc(configs, 'CE_SPLITS_I-INK', 'FORECASTER INPUT')

# COMMAND ----------

tables = ['prod.instant_ink_enrollees', 'mdm.iso_country_code_xref', 'mdm.hardware_xref', 'prod.norm_shipments', 'mdm.calendar']

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table.split(".")[0]
    table_name = table.split(".")[1]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = get_data_by_table(table)
        
    # Write the data to its target.
    df.write \
      .format(write_format) \
      .mode("overwrite") \
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE " + table + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table).createOrReplaceTempView(table_name + "_df_view")

# COMMAND ----------

# duplicate iink enrollees data, once for I-INK and TRAD, with ce_split being calculated
query = '''
    SELECT 
        'CE_SPLITS_I-INK' as record
        , platform_subset
        , c.region_5
        , i.country
        , CASE WHEN c.developed_emerging = 'DEVELOPED' THEN 'DM' WHEN c.developed_emerging = 'EMERGING' THEN 'EM' END em_dm
        , 'I-INK' business_model
        , year_month
        , 'I-INK' split_name 
        , 'PRE' pre_post_flag
        , CASE WHEN
                SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset, i.country ORDER BY year_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= 0 THEN 0 
                ELSE cum_enrollees_month/SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset,i.country ORDER BY year_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
                END AS ce_split 
    FROM  instant_ink_enrollees_df_view i
    LEFT JOIN iso_country_code_xref_df_view c 
    ON c.country_alpha2 = i.country
    WHERE i.official = 1 

    UNION

    SELECT 
        'CE_SPLITS_I-INK' record
        , platform_subset
        , c.region_5
        , i.country
        , CASE WHEN c.developed_emerging = 'DEVELOPED' THEN 'DM' WHEN c.developed_emerging = 'EMERGING' THEN 'EM' END em_dm
        , 'I-INK' business_model
        , year_month
        , 'TRAD' split_name 
        , 'PRE' pre_post_flag
        , CASE WHEN
            SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset, i.country ORDER BY year_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= 0 THEN 0
            ELSE 1 - cum_enrollees_month/SUM(printer_sell_out_units) OVER(PARTITION BY platform_subset,i.country ORDER BY year_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            END AS ce_split
    FROM  instant_ink_enrollees_df_view i
    LEFT JOIN iso_country_code_xref_df_view c
    ON c.country_alpha2 = i.country
    WHERE i.official = 1  
    
        UNION

    SELECT 
        'CE_SPLITS_I-INK' record
        , platform_subset
        , c.region_5
        , i.country
        , CASE WHEN c.developed_emerging = 'DEVELOPED' THEN 'DM' WHEN c.developed_emerging = 'EMERGING' THEN 'EM' END em_dm
        , 'I-INK' business_model
        , year_month
        , 'I-INK' split_name 
        , 'PRE' pre_post_flag
        , 1 ce_split
    FROM  hw_ltf i
    LEFT JOIN iso_country_code_xref_df_view c
    ON c.country_alpha2 = i.country
    WHERE i.official = 1  
'''
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")

initial_ce_split = spark.sql(query)
spark.sql("DROP TABLE IF EXISTS stage.initial_ce_split")
initial_ce_split.write.format("delta").saveAsTable("stage.initial_ce_split")
initial_ce_split.createOrReplaceTempView("initial_ce_split_df_view")

# COMMAND ----------

######################GET PLATFORM SUBSETS BY COUNTRY WHERE CE SPLITS FROM  I-INK ARE LESS THAN 0#####################
query ='''
  SELECT DISTINCT platform_subset,
  country,
  year_month
  FROM stage.initial_ce_split
  WHERE ce_split <= 0 and split_name = 'I-INK'
  '''
ce_less_zero=spark.sql(query)
spark.sql("DROP TABLE IF EXISTS stage.ce_less_zero")
ce_less_zero.write.format("delta").saveAsTable("stage.ce_less_zero")
ce_less_zero.createOrReplaceTempView("ce_less_zero_df_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO stage.initial_ce_split AS d 
# MAGIC using (SELECT platform_subset,country,year_month FROM  stage.ce_less_zero) AS k 
# MAGIC ON d.platform_subset = k.platform_subset 
# MAGIC   AND d.country = k.country 
# MAGIC   AND d.year_month = k.year_month
# MAGIC WHEN MATCHED THEN DELETE 

# COMMAND ----------

############################GET ALL PLATFORM SUBSETS WHERE I-INK GREATER THAN 1###########################
from pyspark.sql.functions import lit
query = '''
  SELECT DISTINCT platform_subset,country,year_month
  FROM stage.initial_ce_split
  WHERE ce_split > 1 and split_name = 'I-INK'
  '''
ce_greater_one = spark.sql(query)
spark.sql("DROP TABLE IF EXISTS stage.ce_greater_one")
ce_greater_one.write.format("delta").saveAsTable("stage.ce_greater_one")
ce_greater_one.createOrReplaceTempView("ce_greater_one_df_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC --SET CORRESPONDING TRAD CE SPLITS (FROM ABOVE BLOCK) FOR I-INK SPLITS GREATER THAN 0
# MAGIC MERGE INTO stage.initial_ce_split AS d 
# MAGIC using (SELECT platform_subset,country,year_month FROM  stage.ce_greater_one) AS k 
# MAGIC ON d.platform_subset = k.platform_subset 
# MAGIC   AND d.country = k.country 
# MAGIC   AND d.year_month = k.year_month
# MAGIC   AND d.split_name = 'TRAD'
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC set d.ce_split = 0;
# MAGIC 
# MAGIC 
# MAGIC UPDATE stage.initial_ce_split SET ce_split = 1 WHERE ce_split > 1 and split_name = 'I-INK'

# COMMAND ----------

initial_ce_split_df = spark.sql('''select * from stage.initial_ce_split''')
initial_ce_split_df.createOrReplaceTempView('initial_ce_split_df_view')

# COMMAND ----------

query = '''
    SELECT DISTINCT
        ce.platform_subset
        , ce.split_name
        , ce.country
        , MIN(ce.year_month) AS min_date
    FROM initial_ce_split_df_view ce
    LEFT JOIN hardware_xref_df_view hw ON ce.platform_subset=hw.platform_subset
    WHERE hw.technology ='INK'
    GROUP BY ce.platform_subset, ce.split_name, ce.country
'''

ce = spark.sql(query)
ce.createOrReplaceTempView('ce_view')

# COMMAND ----------

query1 = '''
    SELECT
        norm.platform_subset
        , norm.country_alpha2
        , MIN(norm.cal_date) AS min_date
        , SUM(norm.units) AS units
    FROM norm_shipments_df_view norm
    LEFT JOIN hardware_xref_df_view hw ON hw.platform_subset = norm.platform_subset
    WHERE norm.version = (SELECT MAX(version) FROM norm_shipments_df_view)
        AND hw.technology = 'INK' 
        AND (norm.platform_subset like '%I-INK' or norm.platform_subset LIKE '%YET%')
    GROUP BY norm.platform_subset, norm.country_alpha2
'''

norm = spark.sql(query1)
norm.createOrReplaceTempView('norm_view')

# COMMAND ----------

current_date = str(date.today())

query2 = f'''
    SELECT DISTINCT
        norm.platform_subset
        , hw.predecessor
        , iso.region_5
        , norm.country_alpha2
        , norm.min_date
        , ROUND(norm.units, 0) AS units 
    FROM norm_view norm
    FULL OUTER JOIN ce_view ce ON ce.platform_subset=norm.platform_subset AND ce.country=norm.country_alpha2
    LEFT JOIN iso_country_code_xref_df_view iso ON iso.country_alpha2=norm.country_alpha2
    LEFT JOIN hardware_xref_df_view hw ON hw.platform_subset=norm.platform_subset
    WHERE ce.platform_subset IS NULL
        AND norm.min_date > '{current_date}'
    GROUP BY norm.platform_subset, hw.predecessor, iso.region_5, norm.country_alpha2, norm.min_date, norm.units, ce.platform_subset, ce.country
'''

tot = spark.sql(query2)
tot.createOrReplaceTempView('tot_view')

# COMMAND ----------

query3 = '''
    SELECT 
        tot.platform_subset
        , tot.predecessor
        , tot.region_5
        , tot.country_alpha2
        , tot.min_date AS npi_start_date
        , tot.units
        , ce.split_name
        , ce.min_date AS pred_start_date
        , months_between(tot.min_date, ce.min_date) AS month_offset
    FROM tot_view tot
    LEFT JOIN ce_view ce
    ON ce.platform_subset=tot.predecessor AND ce.country=tot.country_alpha2
'''

comp = spark.sql(query3)
comp.createOrReplaceTempView('comp_view')

# COMMAND ----------

##################################LOAD CE SPLITS FROM ENROLEES AND NPI INTO ONE TEMP TABLE######################################
query = '''
SELECT 
    'CE_SPLITS_I-INK' record
    , comp.platform_subset
    , comp.region_5
    , comp.country_alpha2
    , ces.em_dm
    , ces.business_model
    , add_months(ces.year_month, comp.month_offset) AS month_begin
    , comp.split_name
    , ces.pre_post_flag
    , ces.ce_split value
FROM comp_view as comp
INNER JOIN initial_ce_split_df_view ces
ON ces.platform_subset=comp.predecessor
    AND comp.country_alpha2=ces.country
    AND comp.split_name=ces.split_name
WHERE ces.record = 'CE_SPLITS_I-INK'

UNION 

SELECT
    'CE_SPLITS_I-INK' record
    , platform_subset
    , region_5
    , country
    , em_dm
    , business_model
    , year_month
    , split_name
    , pre_post_flag
    , ce_split
FROM initial_ce_split_df_view 
'''

npi_plus_enrolles_ce_splits = spark.sql(query)
npi_plus_enrolles_ce_splits.createOrReplaceTempView('npi_plus_enrolles_ce_splits_view')

# COMMAND ----------

######################################CALCULATE FUTURE 15 YEARS CE SPLITS##############################################

####################################GET MAX YEAR MONTH BY PLATFORM, SPLIT NAME AND COUNTRY#####################################
query = '''
SELECT
    MAX(month_begin) AS max_year_month
    , platform_subset
    , country_alpha2
    , split_name
FROM npi_plus_enrolles_ce_splits_view
GROUP BY platform_subset, country_alpha2, split_name
'''

max_month_per_ps_country = spark.sql(query)
max_month_per_ps_country.createOrReplaceTempView('max_month_per_ps_country_view')

# COMMAND ----------

#####################GET CORRESPONDING VALUE AND OTHER ATRIBUTES FOR THE LATEST MONTH FOR PLATFORM SUBSTE BY COUNTRY AND SPLIT NAME################
query = '''
SELECT
    d.max_year_month
    , d.platform_subset
    , d.country_alpha2
    , d.split_name
    , t.value
    , t.em_dm
    , t.business_model
    , t.pre_post_flag
    , t.region_5
FROM max_month_per_ps_country_view d
LEFT JOIN npi_plus_enrolles_ce_splits_view t
ON t.platform_subset = d.platform_subset
    AND t.country_alpha2 = d.country_alpha2
    AND t.split_name = d.split_name
    AND t.month_begin = d.max_year_month
'''

ce_ps_max_per_month_country = spark.sql(query)
ce_ps_max_per_month_country.createOrReplaceTempView('ce_ps_max_per_month_country_view')

# COMMAND ----------

####################LOAD ALL FUTURE CE SPLITS BY PLATFROM SUBSET AND COUNTRY###################################
query = '''
SELECT
    c.Date
    , d.platform_subset
    , d.country_alpha2
    , d.split_name
    , d.value
    , d.em_dm
    , d.business_model
    , d.pre_post_flag
    , region_5
FROM ce_ps_max_per_month_country_view d 
LEFT JOIN calendar_df_view c 
ON 1 = 1
WHERE c.Day_of_Month = 1 
AND (c.Date BETWEEN d.max_year_month AND add_months(d.max_year_month, 15*12))
ORDER BY platform_subset, country_alpha2, split_name, date
'''

ce_future = spark.sql(query)
ce_future.createOrReplaceTempView('ce_future_view')

# COMMAND ----------

######################FINAL CE##############################
query = '''
SELECT 
    'CE_SPLITS_I-INK' AS record
    , platform_subset
    , region_5,country_alpha2
    , em_dm,business_model
    , month_begin
    , split_name
    , pre_post_flag
    , value
FROM npi_plus_enrolles_ce_splits_view

UNION

SELECT
    'CE_SPLITS_I-INK' AS record
    , platform_subset
    , region_5
    , country_alpha2
    , em_dm
    , business_model
    , Date
    , split_name
    , pre_post_flag
    , value
FROM ce_future_view
'''

final_ce = spark.sql(query)
final_ce.createOrReplaceTempView('final_ce_view')

# COMMAND ----------

final_ce = final_ce \
    .select('record', 'platform_subset', 'region_5', 'country_alpha2', 'em_dm', 'business_model',
            'month_begin', 'split_name', 'pre_post_flag', 'value') \
    .withColumn('official', f.lit(1)) \
    .withColumn('load_date', f.lit(max_version_info[1])) \
    .withColumn('version', f.lit(max_version_info[0]))

# COMMAND ----------

update_official_query = """
UPDATE prod.ce_splits 
SET official = 0
FROM mdm.hardware_xref hw 
INNER JOIN prod.ce_splits ce
ON ce.platform_subset = hw.platform_subset
WHERE hw.technology = 'INK' AND ce.official = 1;
"""

submit_remote_query(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'], update_official_query)

# COMMAND ----------

# write data to redshift
write_df_to_redshift(configs, final_ce, "prod.ce_splits", "append")
