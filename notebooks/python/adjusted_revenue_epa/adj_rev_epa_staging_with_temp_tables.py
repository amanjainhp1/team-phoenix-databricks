# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue EPA

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add Version

# COMMAND ----------

add_record = 'ACTUALS - ADJUSTED_REVENUE_EPA'
add_source = 'COMPUTATION FROM ACTUALS FOR EPA DRIVERS'

# COMMAND ----------

adj_flsh_add_version = call_redshift_addversion_sproc(configs, add_record, add_source)

version = adj_flsh_add_version[0]
print(version)

# COMMAND ----------

# Cells 7-8: Refreshes version table in delta lakes, to bring in new version from previous step, above.
version_df = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

actuals_supplies_baseprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_baseprod") \
    .load()

adjusted_revenue_salesprod = read_redshift_to_df(configs) \
    .option("query", "SELECT * FROM fin_prod.adjusted_revenue_salesprod WHERE version = (SELECT max(version) FROM fin_prod.adjusted_revenue_salesprod)") \
    .load()

calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()

iso_country_code_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.iso_country_code_xref") \
    .load()

product_line_xref = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.product_line_xref") \
    .load()

rdma_base_to_sales_product_map = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.rdma_base_to_sales_product_map") \
    .load()

yields = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.yield") \
    .load()

tables = [
    ['prod.version', version_df, "overwrite"], 
    ['fin_prod.actuals_supplies_baseprod', actuals_supplies_baseprod, "overwrite"],
    ['fin_prod.adjusted_revenue_salesprod', adjusted_revenue_salesprod, "overwrite"],
    ['mdm.calendar', calendar, "overwrite"],
    ['mdm.iso_country_code_xref', iso_country_code_xref, "overwrite"],
    ['mdm.product_line_xref', product_line_xref, "overwrite"],
    ['mdm.rdma_base_to_sales_product_map', rdma_base_to_sales_product_map, "overwrite"],
    ['mdm.yields', yields, "overwrite"]
]

# COMMAND ----------

# MAGIC %run "../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step One
# MAGIC Bring in adjusted_revenue_salesprod

# COMMAND ----------

adj_rev_fin = spark.sql("""
SELECT 
		cal_date,
		country_alpha2,
		geography as market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue,
        SUM(net_revenue) + SUM(currency_impact) + SUM(cc_net_revenue) +	SUM(inventory_usd) + SUM(prev_inv_usd) + SUM(inventory_qty) +
        SUM(prev_inv_qty) +	SUM(monthly_unit_change) + SUM(monthly_inv_usd_change) +	SUM(inventory_change_impact) +
        SUM(currency_impact_ch_inventory) +	SUM(cc_inventory_impact) +	SUM(adjusted_revenue) AS total_sums_cleanup
	FROM fin_prod.adjusted_revenue_salesprod
	WHERE 1=1
		AND version = (SELECT max(version) FROM fin_prod.adjusted_revenue_salesprod)
		AND pl NOT IN ('UK', 'TX') 
	GROUP BY cal_date,
		country_alpha2,
		geography,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
""")

adj_rev_fin.createOrReplaceTempView("adjusted_revenue_finance")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_fin.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adjusted_revenue_finance_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adjusted_revenue_finance_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adjusted_revenue_finance_temp'")

# COMMAND ----------

adj_rev_cleaned = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM adjusted_revenue_finance
	WHERE 1=1
		AND total_sums_cleanup <> 0
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement

""")

adj_rev_cleaned.createOrReplaceTempView("adjusted_revenue_cleaned")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_cleaned.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adjusted_revenue_cleaned_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adjusted_revenue_cleaned_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adjusted_revenue_cleaned_temp'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Two
# MAGIC Aggregate ARS data to determine "official targets"

# COMMAND ----------

#DELETE ME LATER? (POST DECISION ABOUT TIME PERIODS FOR TIE OUT TO OFFICIAL FINANCIALS)
two_yrs = spark.sql("""
SELECT 
	cast(Fiscal_Yr-2 as int) as Start_Fiscal_Yr
FROM mdm.calendar
WHERE Date = (select current_date())
""")

two_yrs.createOrReplaceTempView("two_years")

# COMMAND ----------

#exclude tie outs past 2 years of financial restatements, using the code block above
fin_adj_rev_targets_official = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8      
      ,region_5
      ,pl
      ,customer_engagement
	  ,SUM(net_revenue) AS net_revenue
	  ,SUM(cc_net_revenue) AS cc_net_revenue
	  ,SUM(inventory_change_impact) AS inventory_change_impact
	  ,SUM(cc_inventory_impact) AS cc_inventory_impact
	  ,SUM(adjusted_revenue) AS adjusted_revenue
  FROM adjusted_revenue_cleaned
  WHERE 1=1
	AND sales_product_number <> 'EDW_TIE_TO_PLANET'
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,pl
      ,customer_engagement

	UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,pl
      ,customer_engagement
	  ,SUM(net_revenue) AS net_revenue
	  ,SUM(cc_net_revenue) AS cc_net_revenue
	  ,SUM(inventory_change_impact) AS inventory_change_impact
	  ,SUM(cc_inventory_impact) AS cc_inventory_impact
	  ,SUM(adjusted_revenue) AS adjusted_revenue
  FROM adjusted_revenue_cleaned ar
  LEFT JOIN mdm.calendar cal ON cal.Date = ar.cal_date
  WHERE 1=1
	AND Day_of_month = 1
	AND sales_product_number = 'EDW_TIE_TO_PLANET'
	AND Fiscal_Yr >= (SELECT Start_Fiscal_Yr FROM two_years)
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,pl
      ,customer_engagement

    UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,pl
      ,customer_engagement
	  ,SUM(net_revenue) AS net_revenue
	  ,SUM(cc_net_revenue) AS cc_net_revenue
	  ,SUM(inventory_change_impact) AS inventory_change_impact
	  ,SUM(cc_inventory_impact) AS cc_inventory_impact
	  ,SUM(adjusted_revenue) AS adjusted_revenue
  FROM adjusted_revenue_cleaned ar
  WHERE 1=1
	AND pl = 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,pl
      ,customer_engagement
""")

fin_adj_rev_targets_official.createOrReplaceTempView("fin_adj_rev_targets_official")

# COMMAND ----------

# Write out to its delta table target.
fin_adj_rev_targets_official.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/fin_adj_rev_targets_official_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.fin_adj_rev_targets_official_temp USING DELTA LOCATION '/tmp/delta/fin_stage/fin_adj_rev_targets_official_temp'")

# COMMAND ----------

epa_01_adj_rev_targets = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    , sum(net_revenue) as reported_revenue
    , sum(cc_net_revenue) as cc_net_revenue
    , sum(inventory_change_impact) as inventory_change_impact
    , sum(cc_inventory_impact) as cc_inventory_impact
    , sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.fin_adj_rev_targets_official_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl   
"""

query_list.append(["scen.epa_01_adj_rev_targets", epa_01_adj_rev_targets, "overwrite"])

# COMMAND ----------

fin_adj_rev_targets_usa = spark.sql("""
SELECT 
      cal_date
      ,market8
      ,pl
      ,customer_engagement
	  ,COALESCE(SUM(net_revenue), 0) AS net_revenue
	  ,COALESCE(SUM(cc_net_revenue), 0) AS cc_net_revenue
	  ,COALESCE(SUM(inventory_change_impact), 0) AS inventory_change_impact
	  ,COALESCE(SUM(cc_inventory_impact), 0) AS cc_inventory_impact
	  ,COALESCE(SUM(adjusted_revenue), 0) AS adjusted_revenue
  FROM fin_adj_rev_targets_official 
  WHERE 1=1
  AND country_alpha2 = 'US'
  GROUP BY  cal_date
      ,market8
      ,pl
      ,customer_engagement
""")

fin_adj_rev_targets_usa.createOrReplaceTempView("fin_adj_rev_targets_usa")

# COMMAND ----------

fin_adj_rev_targets_non_usa = spark.sql("""
SELECT 
      cal_date
      ,market8
      ,pl
      ,customer_engagement
	  ,COALESCE(SUM(net_revenue), 0) AS net_revenue
	  ,COALESCE(SUM(cc_net_revenue), 0) AS cc_net_revenue
	  ,COALESCE(SUM(inventory_change_impact), 0) AS inventory_change_impact
	  ,COALESCE(SUM(cc_inventory_impact), 0) AS cc_inventory_impact
	  ,COALESCE(SUM(adjusted_revenue), 0) AS adjusted_revenue
  FROM fin_adj_rev_targets_official 
  WHERE 1=1
  AND country_alpha2 <> 'US'
  GROUP BY  cal_date
      ,market8
      ,pl
      ,customer_engagement
""")

fin_adj_rev_targets_non_usa.createOrReplaceTempView("fin_adj_rev_targets_non_usa")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Three
# MAGIC Remove dropped data per filters

# COMMAND ----------

adj_rev_positive = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM adjusted_revenue_cleaned
	WHERE 1=1
		AND net_revenue > 0
		AND adjusted_revenue > 0
		AND sales_product_number <> 'EDW_TIE_TO_PLANET'
		AND sales_product_number NOT LIKE 'UNK%'
		AND sales_product_number NOT LIKE '%PROXY%'
		AND sales_product_number NOT IN ('BIRDS', 'CTSS', 'CISS', 'EST_MPS_REVENUE_JV') -- for now, drop all of them; will add back what we want later
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
""")

adj_rev_positive.createOrReplaceTempView("adjusted_revenue_positive")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_positive.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adj_rev_positive_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adj_rev_positive_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adj_rev_positive_temp'")

# COMMAND ----------

epa_02_adj_rev_filtered = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    , sum(net_revenue) as reported_revenue
    , sum(cc_net_revenue) as cc_net_revenue
    , sum(inventory_change_impact) as inventory_change_impact
    , sum(cc_inventory_impact) as cc_inventory_impact
    , sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.adj_rev_positive_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl     
"""

query_list.append(["scen.epa_02_adj_rev_filtered", epa_02_adj_rev_filtered, "overwrite"])

# COMMAND ----------

adj_rev_negative = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM adjusted_revenue_cleaned
	WHERE 1=1
		AND net_revenue <= 0
		OR sales_product_number = 'EDW_TIE_TO_PLANET'
	    OR sales_product_number LIKE 'UNK%'
		OR sales_product_number LIKE '%PROXY%'
		OR sales_product_number IN ('Birds', 'CTSS', 'CISS', 'EST_MPS_REVENUE_JV') 
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
""")

adj_rev_negative.createOrReplaceTempView("adjusted_revenue_negative")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_negative.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adj_rev_negative_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adj_rev_negative_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adj_rev_negative_temp'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select fiscal_year_qtr, sum(inventory_change_impact) as inventory_change, count(*) as row_count
# MAGIC from fin_stage.adj_rev_negative_temp temp
# MAGIC left join mdm.calendar cal
# MAGIC   on cal.Date = temp.cal_date
# MAGIC where 1=1
# MAGIC and inventory_change_impact <> 0
# MAGIC and day_of_month = 1
# MAGIC group by fiscal_year_qtr
# MAGIC order by 1;

# COMMAND ----------

epa_02a_adj_rev_filtered_out = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    , sum(net_revenue) as reported_revenue
    , sum(cc_net_revenue) as cc_net_revenue
    , sum(inventory_change_impact) as inventory_change_impact
    , sum(cc_inventory_impact) as cc_inventory_impact
    , sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.adj_rev_negative_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl     
"""

query_list.append(["scen.epa_02a_adj_rev_filtered_out", epa_02a_adj_rev_filtered_out, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Four
# MAGIC Spread GD P&L data to GD units

# COMMAND ----------

adj_rev_excl_gd = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM adjusted_revenue_positive
	WHERE 1=1
		AND pl <> 'GD'
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
""")

adj_rev_excl_gd.createOrReplaceTempView("adjusted_revenue_excluding_GD")

# COMMAND ----------

adj_rev_gd = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		--sales_product_number, -- sales product number will be assigned based upon unit mix; prior to this, the only GD sales prod nbr is "edw_tie_to_planet"
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM adjusted_revenue_cleaned
	WHERE 1=1
		AND pl = 'GD'
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		--sales_product_number,
		pl,
		customer_engagement		
""")

adj_rev_gd.createOrReplaceTempView("adjusted_revenue_GD")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_gd.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adj_rev_plGD_pre_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adj_rev_plGD_pre_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adj_rev_plGD_pre_temp'")

# COMMAND ----------

iink_unit_mix = spark.sql("""
SELECT distinct cal_date,
		market8,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(yield_x_units/base_prod_per_sales_prod_qty) as yield_x_units
	FROM fin_prod.actuals_supplies_baseprod ass
    LEFT JOIN mdm.iso_country_code_xref iso
        ON ass.country_alpha2 = iso.country_alpha2
    LEFT JOIN mdm.rdma_base_to_sales_product_map rdma
        ON rdma.base_product_number = ass.base_product_number
	WHERE 1=1
	AND pl = 'GD'
	AND revenue_units > 0
    AND market8 is not null
	GROUP BY cal_date,
		market8,
		sales_product_number,
		pl,
		customer_engagement
""")

iink_unit_mix.createOrReplaceTempView("iink_unit_mix")


iink_unit_mix2 = spark.sql("""
SELECT 
		cal_date,
		market8,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(yield_x_units) as yield_x_units,	
		CASE
			WHEN SUM(yield_x_units) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
			ELSE yield_x_units / SUM(yield_x_units) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
		END AS cc_mix
	FROM iink_unit_mix ium
	WHERE 1=1
	AND pl = 'GD'
	GROUP BY cal_date,
		market8,
		sales_product_number,
		pl,
		customer_engagement,
        yield_x_units
""")

iink_unit_mix2.createOrReplaceTempView("iink_unit_mix2")

# COMMAND ----------

adj_rev_gd_add_skus =  spark.sql("""
SELECT
		gd.cal_date,
		gd.country_alpha2,
		gd.market8,
		gd.region_5,
		iink.sales_product_number,
		gd.pl,
		gd.customer_engagement,
		COALESCE(SUM(net_revenue * cc_mix), 0) AS net_revenue,
		COALESCE(SUM(currency_impact * cc_mix), 0) AS currency_impact,
		COALESCE(SUM(cc_net_revenue * cc_mix), 0) AS cc_net_revenue,
		COALESCE(SUM(inventory_usd * cc_mix), 0) AS inventory_usd,
		COALESCE(SUM(prev_inv_usd * cc_mix), 0) AS prev_inv_usd,
		COALESCE(SUM(inventory_qty * cc_mix), 0) AS inventory_qty,
		COALESCE(SUM(prev_inv_qty * cc_mix), 0) AS prev_inv_qty,
		COALESCE(SUM(monthly_unit_change * cc_mix), 0) AS monthly_unit_change,
		COALESCE(SUM(monthly_inv_usd_change * cc_mix), 0) AS monthly_inv_usd_change,
		COALESCE(SUM(inventory_change_impact * cc_mix), 0) AS inventory_change_impact,
		COALESCE(SUM(currency_impact_ch_inventory * cc_mix), 0) AS currency_impact_ch_inventory,
		COALESCE(SUM(cc_inventory_impact * cc_mix), 0) AS cc_inventory_impact,
		COALESCE(SUM(adjusted_revenue * cc_mix), 0) AS adjusted_revenue 
	FROM adjusted_revenue_GD gd
	INNER JOIN iink_unit_mix2 iink ON
		gd.cal_date = iink.cal_date AND
		gd.market8 = iink.market8 AND
		gd.pl = iink.pl AND
		gd.customer_engagement = iink.customer_engagement
	WHERE 1=1
	GROUP BY 
		gd.cal_date,
		gd.country_alpha2,
		gd.market8,
		gd.region_5,
		iink.sales_product_number,
		gd.pl,
		gd.customer_engagement
""")

adj_rev_gd_add_skus.createOrReplaceTempView("adjusted_revenue_GD_add_skus")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_gd_add_skus.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adj_rev_gd_add_skus_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adj_rev_gd_add_skus_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adj_rev_gd_add_skus_temp'")

# COMMAND ----------

adj_rev_with_iink_sku_pnl = spark.sql("""
WITH
	
	adjusted_revenue_plus_iink_units AS
	(
	SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM adjusted_revenue_excluding_GD
	WHERE 1=1
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement

	UNION ALL

	SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM  adjusted_revenue_GD_add_skus
	WHERE 1=1
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
)

	SELECT
		cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(net_revenue) AS net_revenue,
		SUM(currency_impact) AS currency_impact,
		SUM(cc_net_revenue) AS cc_net_revenue,
		SUM(inventory_usd) AS inventory_usd,
		SUM(prev_inv_usd) AS prev_inv_usd,
		SUM(inventory_qty) AS inventory_qty,
		SUM(prev_inv_qty) AS prev_inv_qty,
		SUM(monthly_unit_change) AS monthly_unit_change,
		SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
		SUM(inventory_change_impact) AS inventory_change_impact,
		SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
		SUM(cc_inventory_impact) AS cc_inventory_impact,
		SUM(adjusted_revenue) AS adjusted_revenue 
	FROM  adjusted_revenue_plus_iink_units 
	WHERE 1=1
	GROUP BY cal_date,
		country_alpha2,
		market8,
		region_5,
		sales_product_number,
		pl,
		customer_engagement	
""")

adj_rev_with_iink_sku_pnl.createOrReplaceTempView("adjusted_revenue_with_iink_sku_pnl")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_with_iink_sku_pnl.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adj_rev_with_iink_sku_pnl_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adj_rev_with_iink_sku_pnl_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adj_rev_with_iink_sku_pnl_temp'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Five
# MAGIC Sales to Base conversion

# COMMAND ----------

rdma_map = spark.sql("""
WITH		
    rdma_map AS
    (
        SELECT 
            sales_product_number,
            CASE
                WHEN sales_product_line_code = 'GM' THEN 'K6'
                WHEN sales_product_line_code = 'EO' THEN 'GL'
                WHEN sales_product_line_code = '65' THEN 'UD'
                ELSE sales_product_line_code
            END AS sales_product_line_code,
            base_product_number,
            CASE
                WHEN base_product_line_code = 'GM' THEN 'K6'
                WHEN base_product_line_code = 'EO' THEN 'GL'
                WHEN base_product_line_code = '65' THEN 'UD'
                ELSE base_product_line_code
            END AS base_product_line_code,
            base_prod_per_sales_prod_qty,
            base_product_amount_percent
        FROM mdm.rdma_base_to_sales_product_map
    ),
    rdma_map2 AS
    (
        SELECT 
            sales_product_number,
            sales_product_line_code,
            base_product_number,
            CASE	
                WHEN sales_product_number = '3YP79AN' AND sales_product_line_code = '1N'
                THEN '1N'
                ELSE base_product_line_code
            END AS base_product_line_code,
            base_prod_per_sales_prod_qty,
            base_product_amount_percent
        FROM rdma_map
    ),
    rdma_map3 AS
    (
        SELECT 
            sales_product_number,
            sales_product_line_code,
            base_product_number,
            base_product_line_code,
            base_prod_per_sales_prod_qty,
            CASE	
                WHEN base_product_number = 'L0S55A' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'L0S52A' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'L0S49A' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'F6U19A' AND sales_product_number = 'X4E09AN' THEN '10'
                WHEN base_product_number = 'T6L90A' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'T6L86A' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'T6M14A' AND sales_product_number = 'X4E09AN' THEN '10'
                WHEN base_product_number = 'CN684W' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'CN052A' AND sales_product_number = 'X4E09AN' THEN '10'
                WHEN base_product_number = 'CN051A' AND sales_product_number = 'X4E09AN' THEN '10'
                WHEN base_product_number = 'CN050A' AND sales_product_number = 'X4E09AN' THEN '10'
                WHEN base_product_number = 'CN683W' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'CN682W' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'CN681W' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'T6L94A' AND sales_product_number = 'X4E09AN' THEN '5'
                WHEN base_product_number = 'L0S55A' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'L0S52A' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'L0S49A' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'F6U19A' AND sales_product_number = 'X4E11AN' THEN '9'
                WHEN base_product_number = 'T6L90A' AND sales_product_number = 'X4E11AN' THEN '3'
                WHEN base_product_number = 'T6L86A' AND sales_product_number = 'X4E11AN' THEN '3'
                WHEN base_product_number = 'T6M14A' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'CN684W' AND sales_product_number = 'X4E11AN' THEN '5'
                WHEN base_product_number = 'CN045A' AND sales_product_number = 'X4E11AN' THEN '11'
                WHEN base_product_number = 'CN052A' AND sales_product_number = 'X4E11AN' THEN '8'
                WHEN base_product_number = 'CN050A' AND sales_product_number = 'X4E11AN' THEN '8'
                WHEN base_product_number = 'CN683W' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'CN682W' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'CN681W' AND sales_product_number = 'X4E11AN' THEN '6'
                WHEN base_product_number = 'CN051A' AND sales_product_number = 'X4E11AN' THEN '8'
                WHEN base_product_number = 'T6L94A' AND sales_product_number = 'X4E11AN' THEN '3'
                WHEN base_product_number = 'CB435A' AND sales_product_number = 'CB435AE' THEN '100'
                WHEN base_product_number = 'CB435AF' AND sales_product_number = 'CB435AF' THEN '100'
                ELSE base_product_amount_percent
            END AS base_product_amount_percent
        FROM rdma_map2
    )

    SELECT *
    FROM rdma_map3
""")

rdma_map.createOrReplaceTempView("rdma")

# COMMAND ----------

adj_rev_baseprod_conversion = spark.sql("""
SELECT 
				cal_date,
				country_alpha2,
				market8,
				region_5,
				sp.sales_product_number,
				sp.pl AS sales_product_line_code,
				base_product_number,
				base_product_line_code,
				customer_engagement,
				COALESCE(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
				COALESCE(SUM(currency_impact * base_product_amount_percent/100), SUM(currency_impact)) AS currency_impact,
				COALESCE(SUM(cc_net_revenue * base_product_amount_percent/100), SUM(cc_net_revenue)) AS cc_net_revenue,
				COALESCE(SUM(inventory_usd * base_product_amount_percent/100), SUM(inventory_usd)) AS inventory_usd,
				COALESCE(SUM(prev_inv_usd * base_product_amount_percent/100), SUM(prev_inv_usd)) AS prev_inv_usd,
				COALESCE(SUM(inventory_qty * base_product_amount_percent/100), SUM(inventory_qty)) AS inventory_qty,
				COALESCE(SUM(prev_inv_qty * base_product_amount_percent/100), SUM(prev_inv_qty)) AS prev_inv_qty,
				COALESCE(SUM(monthly_unit_change * base_product_amount_percent/100), SUM(monthly_unit_change)) AS monthly_unit_change,
				COALESCE(SUM(monthly_inv_usd_change * base_product_amount_percent/100), SUM(monthly_inv_usd_change)) AS monthly_inv_usd_change,
				COALESCE(SUM(inventory_change_impact * base_product_amount_percent/100), SUM(inventory_change_impact)) AS inventory_change_impact,
				COALESCE(SUM(currency_impact_ch_inventory * base_product_amount_percent/100), SUM(currency_impact_ch_inventory)) AS currency_impact_ch_inventory,
				COALESCE(SUM(cc_inventory_impact * base_product_amount_percent/100), SUM(cc_inventory_impact)) AS cc_inventory_impact,
				COALESCE(SUM(adjusted_revenue * base_product_amount_percent/100), SUM(adjusted_revenue)) AS adjusted_revenue 
			FROM adjusted_revenue_with_iink_sku_pnl AS sp
			INNER JOIN rdma AS r ON 
				sp.sales_product_number = r.sales_product_number
			WHERE 1=1
			GROUP BY cal_date,
				country_alpha2,
				market8,
				region_5,
				sp.sales_product_number,
				sp.pl,
				base_product_number,
				base_product_line_code,
				customer_engagement
""")

adj_rev_baseprod_conversion.createOrReplaceTempView("adjusted_revenue_baseprod_conversion")

# COMMAND ----------

# Write out to its delta table target.
adj_rev_baseprod_conversion.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adj_rev_baseprod_conversion")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adj_rev_baseprod_conversion USING DELTA LOCATION '/tmp/delta/fin_stage/adj_rev_baseprod_conversion'")

# COMMAND ----------

epa_03_adj_rev_baseprod_conversion = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , base_product_line_code as pl
    , sum(net_revenue) as reported_revenue
    , sum(cc_net_revenue) as cc_net_revenue
    , sum(inventory_change_impact) as inventory_change_impact
    , sum(cc_inventory_impact) as cc_inventory_impact
    , sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.adj_rev_baseprod_conversion fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , base_product_line_code     
"""

query_list.append(["scen.epa_03_adj_rev_baseprod_conversion", epa_03_adj_rev_baseprod_conversion, "overwrite"])

# COMMAND ----------

salesprod_minus_baseprod = spark.sql("""
SELECT 
				cal_date,
				country_alpha2,
				market8,
				region_5,
				sp.sales_product_number,
				sp.pl AS sales_product_line_code,
				base_product_number,
				base_product_line_code,
				customer_engagement,
				COALESCE(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
				COALESCE(SUM(currency_impact * base_product_amount_percent/100), SUM(currency_impact)) AS currency_impact,
				COALESCE(SUM(cc_net_revenue * base_product_amount_percent/100), SUM(cc_net_revenue)) AS cc_net_revenue,
				COALESCE(SUM(inventory_usd * base_product_amount_percent/100), SUM(inventory_usd)) AS inventory_usd,
				COALESCE(SUM(prev_inv_usd * base_product_amount_percent/100), SUM(prev_inv_usd)) AS prev_inv_usd,
				COALESCE(SUM(inventory_qty * base_product_amount_percent/100), SUM(inventory_qty)) AS inventory_qty,
				COALESCE(SUM(prev_inv_qty * base_product_amount_percent/100), SUM(prev_inv_qty)) AS prev_inv_qty,
				COALESCE(SUM(monthly_unit_change * base_product_amount_percent/100), SUM(monthly_unit_change)) AS monthly_unit_change,
				COALESCE(SUM(monthly_inv_usd_change * base_product_amount_percent/100), SUM(monthly_inv_usd_change)) AS monthly_inv_usd_change,
				COALESCE(SUM(inventory_change_impact * base_product_amount_percent/100), SUM(inventory_change_impact)) AS inventory_change_impact,
				COALESCE(SUM(currency_impact_ch_inventory * base_product_amount_percent/100), SUM(currency_impact_ch_inventory)) AS currency_impact_ch_inventory,
				COALESCE(SUM(cc_inventory_impact * base_product_amount_percent/100), SUM(cc_inventory_impact)) AS cc_inventory_impact,
				COALESCE(SUM(adjusted_revenue * base_product_amount_percent/100), SUM(adjusted_revenue)) AS adjusted_revenue 
			FROM adjusted_revenue_with_iink_sku_pnl AS sp
			LEFT JOIN rdma AS r ON 
				sp.sales_product_number = r.sales_product_number
			WHERE 1=1
				AND sp.sales_product_number NOT IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'EDW_TIE_TO_PLANET')
				AND base_product_number IS NULL
			GROUP BY cal_date,
				country_alpha2,
				market8,
				region_5,
				sp.sales_product_number,
				sp.pl,
				base_product_number,
				base_product_line_code,
				customer_engagement
""")

salesprod_minus_baseprod.createOrReplaceTempView("salesprod_missing_baseprod")

# COMMAND ----------

# Write out to its delta table target.
salesprod_minus_baseprod.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/salesprod_missing_baseprod_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.salesprod_missing_baseprod_temp USING DELTA LOCATION '/tmp/delta/fin_stage/salesprod_missing_baseprod_temp'")

# COMMAND ----------

epa_03a_adj_rev_baseprod_conversion_dropout = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , base_product_line_code as pl
    , sum(net_revenue) as reported_revenue
    , sum(cc_net_revenue) as cc_net_revenue
    , sum(inventory_change_impact) as inventory_change_impact
    , sum(cc_inventory_impact) as cc_inventory_impact
    , sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.salesprod_missing_baseprod_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , base_product_line_code     
"""

query_list.append(["scen.epa_03a_adj_rev_baseprod_conversion_dropout", epa_03a_adj_rev_baseprod_conversion_dropout, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Six
# MAGIC Drop Unknowns

# COMMAND ----------

adj_rev_base_product_data = spark.sql("""
WITH baseprod_data AS
		(	
			SELECT 
				cal_date,
				country_alpha2,
				market8,
				region_5,
				base_product_number,
				base_product_line_code,
				customer_engagement,
				SUM(net_revenue) AS net_revenue,
				SUM(currency_impact) AS currency_impact,
				SUM(cc_net_revenue) AS cc_net_revenue,
				SUM(inventory_usd) AS inventory_usd,
				SUM(prev_inv_usd) AS prev_inv_usd,
				SUM(inventory_qty) AS inventory_qty,
				SUM(prev_inv_qty) AS prev_inv_qty,
				SUM(monthly_unit_change) AS monthly_unit_change,
				SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
				SUM(inventory_change_impact) AS inventory_change_impact,
				SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
				SUM(cc_inventory_impact) AS cc_inventory_impact,
				SUM(adjusted_revenue) AS adjusted_revenue
			FROM adjusted_revenue_baseprod_conversion
			GROUP BY cal_date, country_alpha2, base_product_line_code, customer_engagement, base_product_number, market8, region_5
		)
				
			SELECT 
				cal_date,
				country_alpha2,
				market8,
				region_5,
				base_product_number,
				base_product_line_code AS pl,
				customer_engagement,
				SUM(net_revenue) AS net_revenue,
				SUM(currency_impact) AS currency_impact,
				SUM(cc_net_revenue) AS cc_net_revenue,
				SUM(inventory_usd) AS inventory_usd,
				SUM(prev_inv_usd) AS prev_inv_usd,
				SUM(inventory_qty) AS inventory_qty,
				SUM(prev_inv_qty) AS prev_inv_qty,
				SUM(monthly_unit_change) AS monthly_unit_change,
				SUM(monthly_inv_usd_change) AS monthly_inv_usd_change,
				SUM(inventory_change_impact) AS inventory_change_impact,
				SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
				SUM(cc_inventory_impact) AS cc_inventory_impact,
				SUM(adjusted_revenue) AS adjusted_revenue
			FROM baseprod_data
			WHERE base_product_line_code IN 
				(	
					SELECT distinct pl 
					FROM mdm.product_line_xref 
					WHERE PL_category IN ('SUP')
						AND Technology IN ('INK', 'LASER', 'PWA')
						AND pl NOT IN ('GY', 'LZ')
				)
			GROUP BY cal_date, country_alpha2, base_product_line_code, customer_engagement, base_product_number, market8, region_5
""")

adj_rev_base_product_data.createOrReplaceTempView("adjusted_revenue_base_product_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Seven
# MAGIC Pull in official actuals (ASB) and do mkt 8 clean up

# COMMAND ----------

orig_official_fin_prep1 = spark.sql("""
SELECT 
      cal_date
      ,baseprod.country_alpha2
      ,baseprod.market10
	  ,market8
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM fin_prod.actuals_supplies_baseprod baseprod
  LEFT JOIN mdm.iso_country_code_xref iso 
      ON baseprod.country_alpha2 = iso.country_alpha2
  WHERE 1=1
	AND pl IN (select distinct pl from adjusted_revenue_finance)
	AND customer_engagement <> 'EST_DIRECT_FULFILLMENT'
  GROUP BY  cal_date
      , baseprod.country_alpha2
      , baseprod.market10
	  , market8
      , platform_subset
      , base_product_number
      , pl
      , customer_engagement

""")

orig_official_fin_prep1.createOrReplaceTempView("orig_official_fin_prep1")

# COMMAND ----------

orig_official_fin_prep2 = spark.sql("""
 	SELECT 
      cal_date
      ,CASE
          WHEN country_alpha2 = 'BY' THEN 'HU'
          WHEN country_alpha2 = 'RU' THEN 'HU'
          WHEN country_alpha2 = 'CU' THEN 'MX'
          WHEN country_alpha2 = 'IR' THEN 'LB'
          WHEN country_alpha2 = 'KP' THEN 'KR'
          WHEN country_alpha2 = 'SY' THEN 'LB'
          ELSE country_alpha2
       END AS country_alpha2
      ,CASE
          WHEN country_alpha2 = 'XW' THEN 'WORLD WIDE'
          WHEN country_alpha2 = 'BY' THEN 'CENTRAL & EASTERN EUROPE'
          WHEN country_alpha2 = 'RU' THEN 'CENTRAL & EASTERN EUROPE'
          WHEN country_alpha2 = 'CU' THEN 'LATIN AMERICA'
          WHEN country_alpha2 = 'IR' THEN 'SOUTHERN EUROPE, ME & AFRICA'
          WHEN country_alpha2 = 'KP' THEN 'GREATER ASIA'
          WHEN country_alpha2 = 'SY' THEN 'SOUTHERN EUROPE, ME & AFRICA'
		ELSE market8
      END AS market8
	  ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM orig_official_fin_prep1
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

orig_official_fin_prep2.createOrReplaceTempView("orig_official_fin_prep2")

# COMMAND ----------

orig_official_fin_prep3 = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,case
			when market8 IN ('CENTRAL & EASTERN EUROPE', 'NORTHWEST EUROPE', 'SOUTHERN EUROPE, ME & AFRICA') then 'EU'
			when market8 IN ('LATIN AMERICA') THEN 'LA'
			when market8 IN ('NORTH AMERICA') then 'NA'
			when market8 IN ('GREATER ASIA', 'GREATER CHINA', 'INDIA SL & BL') then 'AP'
			else 'XW'
		end as region_5
	  ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM orig_official_fin_prep2
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

orig_official_fin_prep3.createOrReplaceTempView("orig_official_fin_prep3")

# COMMAND ----------

orig_official_fin4 = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
	  ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM orig_official_fin_prep3
  WHERE 1=1
  AND base_product_number <> 'EDW_TIE_TO_PLANET'
  AND pl <> 'GD' 
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      
 UNION ALL
 
 SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
	  ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM orig_official_fin_prep3 f
  LEFT JOIN mdm.calendar cal ON cal.Date = f.cal_date
  WHERE 1=1
	AND Day_of_month = 1
	AND base_product_number IN ('EDW_TIE_TO_PLANET')
	AND Fiscal_Yr >= (SELECT Start_Fiscal_Yr FROM two_years)
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      
 UNION ALL
 
 SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
	  ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM orig_official_fin_prep3
  WHERE 1=1
  AND pl = 'GD' 
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

orig_official_fin4.createOrReplaceTempView("original_official_financials")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Eight
# MAGIC Drop edw tieouts that we're excluding (for now, pending REQs) and compute implied yields

# COMMAND ----------

baseprod_detailed_targets = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM original_official_financials
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

baseprod_detailed_targets.createOrReplaceTempView("baseprod_detailed_targets")

# COMMAND ----------

# Write out to its delta table target.
baseprod_detailed_targets.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/baseprod_targets_detailed_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.baseprod_targets_detailed_temp USING DELTA LOCATION '/tmp/delta/fin_stage/baseprod_targets_detailed_temp'")

# COMMAND ----------

epa_04_baseprod_targets = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    ,sum(net_revenue) as net_revenue
    ,sum(revenue_units) as revenue_units
    ,sum(equivalent_units) as equivalent_units
    ,sum(yield_x_units) as yield_x_units
    ,sum(yield_x_units_black_only) as yield_x_units_black_only
FROM fin_stage.baseprod_targets_detailed_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl     
"""

query_list.append(["scen.epa_04_baseprod_targets", epa_04_baseprod_targets, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Nine
# MAGIC Drop negative net revenue and spread PL GD P&L data from ASB to units

# COMMAND ----------

baseprod_detail_positive_unit = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_detailed_targets
  WHERE 1=1
	AND revenue_units >= 0
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

baseprod_detail_positive_unit.createOrReplaceTempView("baseprod_details_positive_units")

# COMMAND ----------

baseprod_official_wo_gd = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_details_positive_units
  WHERE 1=1
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

baseprod_official_wo_gd.createOrReplaceTempView("baseprod_official_wo_GD")

# COMMAND ----------

baseprod_fin_official_w_gd = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,pl
      ,customer_engagement
	  ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_detailed_targets
  WHERE 1=1
	AND pl = 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,pl
      ,customer_engagement
""")

baseprod_fin_official_w_gd.createOrReplaceTempView("baseprod_fin_official_w_GD")

# COMMAND ----------

# Write out to its delta table target.
baseprod_fin_official_w_gd.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/baseprod_fin_official_w_gd_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.baseprod_fin_official_w_gd_temp USING DELTA LOCATION '/tmp/delta/fin_stage/baseprod_fin_official_w_gd_temp'")

# COMMAND ----------

iink_units_mix_base_w_printer = spark.sql("""
SELECT 
		cal_date,
		market8,
		base_product_number,
		platform_subset,
		pl,
		customer_engagement,	
		CASE
			WHEN SUM(yield_x_units) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
			ELSE yield_x_units / SUM(yield_x_units) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
		END AS cc_mix
	FROM fin_prod.actuals_supplies_baseprod asb
    LEFT JOIN mdm.iso_country_code_xref iso 
        ON iso.country_alpha2 = asb.country_alpha2
	WHERE 1=1
	AND pl = 'GD'
	AND revenue_units > 0
	AND platform_subset <> 'NA'
    AND market8 is not null
	GROUP BY cal_date, pl, customer_engagement, base_product_number, market8, yield_x_units, platform_subset
""")

iink_units_mix_base_w_printer.createOrReplaceTempView("iink_units_mix_base_w_printer")

# COMMAND ----------

baseprod_fin_gd_add_sku_data = spark.sql("""
SELECT
		gd.cal_date,
		gd.country_alpha2,
		gd.market8,
		gd.region_5,
		iink.base_product_number,
		iink.platform_subset,
		gd.pl,
		gd.customer_engagement,			
		COALESCE(SUM(cc_mix * gd.net_revenue), 0) AS net_revenue,
		COALESCE(SUM(cc_mix * gd.revenue_units), 0) AS revenue_units,
		COALESCE(SUM(equivalent_units * cc_mix), 0) AS equivalent_units,
		COALESCE(SUM(yield_x_units * cc_mix), 0) AS yield_x_units,
		COALESCE(SUM(yield_x_units_black_only * cc_mix), 0) AS yield_x_units_black_only
	FROM baseprod_fin_official_w_GD gd
	INNER JOIN iink_units_mix_base_w_printer iink ON
		gd.cal_date = iink.cal_date AND
		gd.market8 = iink.market8 AND
		gd.pl = iink.pl AND
		gd.customer_engagement = iink.customer_engagement
	WHERE 1=1
	GROUP BY 
		gd.cal_date,
		gd.country_alpha2,
		gd.market8,
		gd.region_5,
		iink.base_product_number,
		iink.platform_subset,
		gd.pl,
		gd.customer_engagement
""")

baseprod_fin_gd_add_sku_data.createOrReplaceTempView("baseprod_fin_GD_add_sku_data")

# COMMAND ----------

baseprod_data_gd_unit_adjusted = spark.sql("""
WITH financial_data_excluding_GD AS
(
	SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_official_wo_GD
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	 UNION ALL
	
	SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_fin_GD_add_sku_data
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
)

	SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM financial_data_excluding_GD
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

baseprod_data_gd_unit_adjusted.createOrReplaceTempView("baseprod_data_GD_unit_adjusted")

# COMMAND ----------

# Write out to its delta table target.
baseprod_data_gd_unit_adjusted.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/baseprod_data_gd_unit_adjusted_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.baseprod_data_gd_unit_adjusted_temp USING DELTA LOCATION '/tmp/delta/fin_stage/baseprod_data_gd_unit_adjusted_temp'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Ten
# MAGIC Drop unnecessary data, similar to ARS step; set implied NDP, yields

# COMMAND ----------

fin_gold_dataset = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_data_GD_unit_adjusted
  WHERE 1=1
	AND net_revenue > 0
	AND platform_subset <> 'NA'
	AND base_product_number NOT LIKE 'UNK%'
	AND base_product_number NOT LIKE 'EDW%'
	AND base_product_number NOT IN ('BIRDS', 'CTSS', 'CISS', 'EST_MPS_REVENUE_JV')  -- will add back later
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

fin_gold_dataset.createOrReplaceTempView("financials_gold_dataset")

# COMMAND ----------

# Write out to its delta table target.
fin_gold_dataset.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/fin_gold_dataset_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.fin_gold_dataset_temp USING DELTA LOCATION '/tmp/delta/fin_stage/fin_gold_dataset_temp'")

# COMMAND ----------

epa_05_baseprod_positive = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    ,sum(net_revenue) as net_revenue
    ,sum(revenue_units) as revenue_units
    ,sum(equivalent_units) as equivalent_units
    ,sum(yield_x_units) as yield_x_units
    ,sum(yield_x_units_black_only) as yield_x_units_black_only
FROM fin_stage.fin_gold_dataset_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_05_baseprod_positive", epa_05_baseprod_positive, "epa_05_baseprod_positive"])

# COMMAND ----------

fin_dropped_out = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_detailed_targets
  WHERE 1=1
	AND revenue_units < 0
	AND net_revenue <= 0
	AND platform_subset = 'NA'
	AND base_product_number LIKE 'UNK%'
	AND base_product_number LIKE 'EDW%'
	AND base_product_number IN ('BIRDS', 'CTSS', 'CISS', 'EST_MPS_REVENUE_JV')  -- will add back later
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

fin_dropped_out.createOrReplaceTempView("financials_dropped_out")

# COMMAND ----------

# Write out to its delta table target.
fin_dropped_out.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/fin_dropped_out_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.fin_dropped_out_temp USING DELTA LOCATION '/tmp/delta/fin_stage/fin_dropped_out_temp'")

# COMMAND ----------

epa_05a_baseprod_dropout = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    ,sum(net_revenue) as net_revenue
    ,sum(revenue_units) as revenue_units
    ,sum(equivalent_units) as equivalent_units
    ,sum(yield_x_units) as yield_x_units
    ,sum(yield_x_units_black_only) as yield_x_units_black_only
FROM fin_stage.fin_dropped_out_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_05a_baseprod_dropout", epa_05_baseprod_positive, "epa_05a_baseprod_dropout"])

# COMMAND ----------

fin_with_printer_mix_calc = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
	  ,CASE
			WHEN sum(net_revenue) OVER (PARTITION BY cal_date, country_alpha2, base_product_number, pl, customer_engagement) = 0 THEN NULL
			ELSE net_revenue / sum(net_revenue) OVER (PARTITION BY cal_date, country_alpha2, base_product_number, pl, customer_engagement)
		END AS fin_rev_mix
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM financials_gold_dataset
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

fin_with_printer_mix_calc.createOrReplaceTempView("financials_with_printer_mix_calc")

# COMMAND ----------

base_financials_arus_yields = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,sum(fin_rev_mix) as fin_rev_mix
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM financials_with_printer_mix_calc
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

base_financials_arus_yields.createOrReplaceTempView("base_financials_arus_yields")

# COMMAND ----------

# Write out to its delta table target.
base_financials_arus_yields.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/base_financials_arus_yields_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.base_financials_arus_yields_temp USING DELTA LOCATION '/tmp/delta/fin_stage/base_financials_arus_yields_temp'")

# COMMAND ----------

base_financials_arus_yields_usa = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,sum(fin_rev_mix) as fin_rev_mix
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM base_financials_arus_yields
  WHERE 1=1
  AND country_alpha2 = 'US'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

base_financials_arus_yields_usa.createOrReplaceTempView("base_financials_arus_yields_usa")

# COMMAND ----------

base_financials_arus_yields_non_usa = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,sum(fin_rev_mix) as fin_rev_mix
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM base_financials_arus_yields
  WHERE 1=1
  AND country_alpha2 <> 'US'
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

base_financials_arus_yields_non_usa.createOrReplaceTempView("base_financials_arus_yields_non_usa")

# COMMAND ----------

prepped_adj_rev_data = spark.sql("""
SELECT 
    cal_date,
    country_alpha2,
    market8,
    region_5,
    base_product_number,
    pl,
    customer_engagement,
    SUM(currency_impact) AS currency_impact,
    SUM(cc_net_revenue) AS cc_net_revenue,
    SUM(inventory_usd) AS inventory_usd,
    SUM(inventory_qty) AS inventory_qty,
    SUM(inventory_change_impact) AS inventory_change_impact,
    SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
    SUM(cc_inventory_impact) AS cc_inventory_impact,
    SUM(adjusted_revenue) AS adjusted_revenue
FROM adjusted_revenue_base_product_data
WHERE 1=1
GROUP BY cal_date, country_alpha2, pl, customer_engagement, base_product_number, market8, region_5
""")

prepped_adj_rev_data.createOrReplaceTempView("prepped_adj_rev_data")

# COMMAND ----------

prepped_adj_rev_data_usa = spark.sql("""
SELECT distinct
    cal_date,
    country_alpha2,
    market8,
    region_5,
    base_product_number,
    pl,
    customer_engagement,
    SUM(currency_impact) AS currency_impact,
    SUM(cc_net_revenue) AS cc_net_revenue,
    SUM(inventory_usd) AS inventory_usd,
    SUM(inventory_qty) AS inventory_qty,
    SUM(inventory_change_impact) AS inventory_change_impact,
    SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
    SUM(cc_inventory_impact) AS cc_inventory_impact,
    SUM(adjusted_revenue) AS adjusted_revenue
FROM prepped_adj_rev_data
WHERE 1=1
AND country_alpha2 = 'US'
GROUP BY cal_date, country_alpha2, pl, customer_engagement, base_product_number, market8, region_5
""")

prepped_adj_rev_data_usa.createOrReplaceTempView("prepped_adj_rev_data_usa")

# COMMAND ----------

prepped_adj_rev_data_non_usa = spark.sql("""
SELECT distinct
    cal_date,
    country_alpha2,
    market8,
    region_5,
    base_product_number,
    pl,
    customer_engagement,
    SUM(currency_impact) AS currency_impact,
    SUM(cc_net_revenue) AS cc_net_revenue,
    SUM(inventory_usd) AS inventory_usd,
    SUM(inventory_qty) AS inventory_qty,
    SUM(inventory_change_impact) AS inventory_change_impact,
    SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
    SUM(cc_inventory_impact) AS cc_inventory_impact,
    SUM(adjusted_revenue) AS adjusted_revenue
FROM prepped_adj_rev_data
WHERE 1=1
AND country_alpha2 <> 'US'
GROUP BY cal_date, country_alpha2, pl, customer_engagement, base_product_number, market8, region_5
""")

prepped_adj_rev_data_non_usa.createOrReplaceTempView("prepped_adj_rev_data_non_usa")

# COMMAND ----------

baseprod_fin_with_adj_rev_variables_usa = spark.sql("""
SELECT 
      mix.cal_date
      ,mix.country_alpha2
      ,mix.market8
	  ,mix.region_5
      ,platform_subset
      ,mix.base_product_number
      ,mix.pl
      ,mix.customer_engagement
	  ,sum(fin_rev_mix) fin_rev_mix
      ,sum(mix.net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	  ,0 as currency_impact
      ,sum(mix.net_revenue) as cc_net_revenue
	  ,COALESCE(sum(inventory_usd * fin_rev_mix), 0) as ci_usd
	  ,COALESCE(sum(inventory_qty * fin_rev_mix), 0) as ci_qty
	  ,COALESCE(sum(inventory_change_impact * fin_rev_mix), 0) as inventory_change_impact
	  ,COALESCE(sum(cc_inventory_impact * fin_rev_mix), 0) as cc_inventory_impact
  FROM base_financials_arus_yields_usa mix
  LEFT JOIN prepped_adj_rev_data_usa cc_rev ON
	mix.cal_date = cc_rev.cal_date AND
	mix.country_alpha2 = cc_rev.country_alpha2 AND
	mix.market8 = cc_rev.market8 AND
	mix.region_5 = cc_rev.region_5 AND
	mix.base_product_number = cc_rev.base_product_number AND
	mix.pl = cc_rev.pl AND
	mix.customer_engagement = cc_rev.customer_engagement
  WHERE 1=1
	AND net_revenue > 0
  GROUP BY   mix.cal_date
      ,mix.country_alpha2
      ,mix.market8
	  ,mix.region_5
      ,platform_subset
      ,mix.base_product_number
      ,mix.pl
      ,mix.customer_engagement
""")

baseprod_fin_with_adj_rev_variables_usa.createOrReplaceTempView("baseprod_fin_with_adj_rev_variables_usa")

# COMMAND ----------

baseprod_fin_with_adj_rev_variables_non_usa = spark.sql("""
SELECT 
      mix.cal_date
      ,mix.country_alpha2
      ,mix.market8
	  ,mix.region_5
      ,platform_subset
      ,mix.base_product_number
      ,mix.pl
      ,mix.customer_engagement
	  ,sum(fin_rev_mix) fin_rev_mix
      ,sum(mix.net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	  ,COALESCE(sum(currency_impact * fin_rev_mix), 0) as currency_impact
      ,sum(mix.net_revenue) + COALESCE(sum(currency_impact * fin_rev_mix), 0) as cc_net_revenue
	  ,COALESCE(sum(inventory_usd * fin_rev_mix), 0) as ci_usd
	  ,COALESCE(sum(inventory_qty * fin_rev_mix), 0) as ci_qty
	  ,COALESCE(sum(inventory_change_impact * fin_rev_mix), 0) as inventory_change_impact
	  ,COALESCE(sum(cc_inventory_impact * fin_rev_mix), 0) as cc_inventory_impact
  FROM base_financials_arus_yields_non_usa mix
  LEFT JOIN prepped_adj_rev_data_non_usa cc_rev ON
	mix.cal_date = cc_rev.cal_date AND
	mix.country_alpha2 = cc_rev.country_alpha2 AND
	mix.market8 = cc_rev.market8 AND
	mix.region_5 = cc_rev.region_5 AND
	mix.base_product_number = cc_rev.base_product_number AND
	mix.pl = cc_rev.pl AND
	mix.customer_engagement = cc_rev.customer_engagement
  WHERE 1=1
	AND net_revenue > 0
  GROUP BY   mix.cal_date
      ,mix.country_alpha2
      ,mix.market8
	  ,mix.region_5
      ,platform_subset
      ,mix.base_product_number
      ,mix.pl
      ,mix.customer_engagement
""")

baseprod_fin_with_adj_rev_variables_non_usa.createOrReplaceTempView("baseprod_fin_with_adj_rev_variables_non_usa")

# COMMAND ----------

fully_baked_sku_level_data = spark.sql("""
with rejoin_special_cases_with_scrubbed_data AS
(
	SELECT 
     cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	  ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(cc_net_revenue) - sum(cc_inventory_impact) as adjusted_revenue
    FROM baseprod_fin_with_adj_rev_variables_usa
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	UNION ALL

    SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	  ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(cc_net_revenue) - sum(cc_inventory_impact) as adjusted_revenue
    FROM baseprod_fin_with_adj_rev_variables_non_usa
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,'NA' as platform_subset
      ,sales_product_number as base_product_number
      ,pl
      ,customer_engagement
      ,COALESCE(sum(net_revenue), 0) as net_revenue
      ,0 as revenue_units
      ,0 as equivalent_units
      ,0 as yield_x_units
      ,0 as yield_x_units_black_only
	  ,0 as currency_impact
	  ,COALESCE(sum(net_revenue),0) as cc_net_revenue
	  ,0 as ci_usd
	  ,0 as ci_qty
	  ,0 as inventory_change_impact
	  ,0 as cc_inventory_impact
	  ,COALESCE(sum(net_revenue), 0) as adjusted_revenue
  FROM adjusted_revenue_finance
  WHERE 1=1
	AND sales_product_number IN ('CISS', 'EST_MPS_REVENUE_JV')
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,sales_product_number
      ,pl
      ,customer_engagement
)

SELECT 
     cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,COALESCE(sum(net_revenue), 0) as net_revenue
      ,COALESCE(sum(revenue_units), 0) as revenue_units
      ,COALESCE(sum(equivalent_units), 0) as equivalent_units
      ,COALESCE(sum(yield_x_units), 0) as yield_x_units
      ,COALESCE(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
	  ,COALESCE(sum(currency_impact), 0) as currency_impact
	  ,COALESCE(sum(cc_net_revenue), 0) as cc_net_revenue
	  ,COALESCE(sum(ci_usd), 0) as ci_usd
	  ,COALESCE(sum(ci_qty), 0) as ci_qty
	  ,COALESCE(sum(inventory_change_impact), 0) as inventory_change_impact
	  ,COALESCE(sum(cc_inventory_impact), 0) as cc_inventory_impact
	  ,COALESCE(sum(adjusted_revenue), 0) as adjusted_revenue
    FROM rejoin_special_cases_with_scrubbed_data
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

fully_baked_sku_level_data.createOrReplaceTempView("fully_baked_sku_level_data")

# COMMAND ----------

# Write out to its delta table target.
fully_baked_sku_level_data.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/fully_baked_sku_level_data_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.fully_baked_sku_level_data_temp USING DELTA LOCATION '/tmp/delta/fin_stage/fully_baked_sku_level_data_temp'")

# COMMAND ----------

epa_06_fully_joined = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    ,sum(net_revenue) as net_revenue
    ,sum(revenue_units) as revenue_units
    ,sum(equivalent_units) as equivalent_units
    ,sum(yield_x_units) as yield_x_units
    ,sum(yield_x_units_black_only) as yield_x_units_black_only
    ,sum(currency_impact) as currency_impact
	,sum(cc_net_revenue) as cc_net_revenue
	,sum(ci_usd) as ci_usd
	,sum(ci_qty) as ci_qty
	,sum(inventory_change_impact) as inventory_change_impact
	,sum(cc_inventory_impact) as cc_inventory_impact
	,sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.fully_baked_sku_level_data_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_06_fully_joined", epa_06_fully_joined, "epa_06_fully_joined"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Eleven
# MAGIC Spread gap in revenue and cc_revenue caused by dropped data to where we have data 

# COMMAND ----------

reported_mkt8_data_usa = spark.sql("""
SELECT cal_date,
		market8,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		sum(cc_net_revenue) as cc_net_revenue,
		sum(inventory_change_impact) as inventory_change_impact,
	    sum(cc_inventory_impact) as cc_inventory_impact
	FROM fully_baked_sku_level_data
    WHERE country_alpha2 = 'US'
	GROUP BY cal_date, market8, pl, customer_engagement
""")

reported_mkt8_data_usa.createOrReplaceTempView("reported_mkt8_data_usa")

# COMMAND ----------

reported_mkt8_data_non_usa = spark.sql("""
SELECT cal_date,
		market8,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		sum(cc_net_revenue) as cc_net_revenue,
		sum(inventory_change_impact) as inventory_change_impact,
	    sum(cc_inventory_impact) as cc_inventory_impact
	FROM fully_baked_sku_level_data
    WHERE country_alpha2 <> 'US'
	GROUP BY cal_date, market8, pl, customer_engagement
""")

reported_mkt8_data_non_usa.createOrReplaceTempView("reported_mkt8_data_non_usa")

# COMMAND ----------

target_mkt8_data = spark.sql("""
SELECT cal_date,
        market8,
        pl,
        customer_engagement,
        sum(net_revenue) as net_revenue,
        sum(cc_net_revenue) as cc_net_revenue,
        sum(inventory_change_impact) as inventory_change_impact,
        sum(cc_inventory_impact) as cc_inventory_impact
    FROM fin_adj_rev_targets_official
    WHERE 1=1
    AND pl IN (select distinct pl from fully_baked_sku_level_data)
    GROUP BY cal_date, market8, pl, customer_engagement
""")
 
target_mkt8_data.createOrReplaceTempView("target_mkt8_data")

# COMMAND ----------

target_mkt8_data_usa = spark.sql("""
SELECT cal_date,
		market8,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		sum(cc_net_revenue) as cc_net_revenue,
		sum(inventory_change_impact) as inventory_change_impact,
	    sum(cc_inventory_impact) as cc_inventory_impact
	FROM fin_adj_rev_targets_usa
	WHERE 1=1
	AND pl IN (select distinct pl from fully_baked_sku_level_data)
	GROUP BY cal_date, market8, pl, customer_engagement
""")

target_mkt8_data_usa.createOrReplaceTempView("target_mkt8_data_usa")

# COMMAND ----------

target_mkt8_data_non_usa = spark.sql("""
SELECT cal_date,
		market8,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		sum(cc_net_revenue) as cc_net_revenue,
		sum(inventory_change_impact) as inventory_change_impact,
	    sum(cc_inventory_impact) as cc_inventory_impact
	FROM fin_adj_rev_targets_non_usa
	WHERE 1=1
	AND pl IN (select distinct pl from reported_mkt8_data_non_usa)
	GROUP BY cal_date, market8, pl, customer_engagement
""")

target_mkt8_data_non_usa.createOrReplaceTempView("target_mkt8_data_non_usa")

# COMMAND ----------

calc_difference_to_targets_usa = spark.sql("""
SELECT
		r.cal_date,
		r.market8,
		r.pl,
		r.customer_engagement,
		sum(r.net_revenue) as rept_net_rev,
		sum(far.net_revenue) as net_rev_target,
		sum(r.cc_net_Revenue) as rept_cc_rev,
		sum(far.cc_net_revenue) as cc_net_rev_target,
		sum(r.inventory_change_impact) as rept_inv_change,
		sum(far.inventory_change_impact) as inv_chg_target,
		sum(r.cc_inventory_impact) as rept_inv_impact,
		sum(far.cc_inventory_impact) as cc_inv_impact
	FROM reported_mkt8_data_usa r
	LEFT JOIN target_mkt8_data_usa far ON
		r.cal_date = far.cal_date AND
		r.market8 = far.market8 AND
		r.pl = far.pl AND
		r.customer_engagement = far.customer_engagement
	WHERE 1=1
		AND r.net_revenue <> 0
	GROUP BY r.cal_date, r.market8, r.pl, r.customer_engagement

""")

calc_difference_to_targets_usa.createOrReplaceTempView("calc_difference_to_targets_usa")

# COMMAND ----------

calc_difference_to_targets_non_usa = spark.sql("""
SELECT
		r.cal_date,
		r.market8,
		r.pl,
		r.customer_engagement,
		sum(r.net_revenue) as rept_net_rev,
		sum(far.net_revenue) as net_rev_target,
		sum(r.cc_net_Revenue) as rept_cc_rev,
		sum(far.cc_net_revenue) as cc_net_rev_target,
		sum(r.inventory_change_impact) as rept_inv_change,
		sum(far.inventory_change_impact) as inv_chg_target,
		sum(r.cc_inventory_impact) as rept_inv_impact,
		sum(far.cc_inventory_impact) as cc_inv_impact
	FROM reported_mkt8_data_non_usa r
	LEFT JOIN target_mkt8_data_non_usa far ON
		r.cal_date = far.cal_date AND
		r.market8 = far.market8 AND
		r.pl = far.pl AND
		r.customer_engagement = far.customer_engagement
	WHERE 1=1
		AND r.net_revenue <> 0
	GROUP BY r.cal_date, r.market8, r.pl, r.customer_engagement

""")

calc_difference_to_targets_non_usa.createOrReplaceTempView("calc_difference_to_targets_non_usa")

# COMMAND ----------

target_plugs_to_fill_gap_usa = spark.sql("""
SELECT
		cal_date,
		market8,
		pl,
		customer_engagement,
		sum(net_rev_target) - sum(rept_net_rev) as net_rev_plug,
		sum(cc_net_rev_target) - sum(rept_cc_rev) as cc_rev_plug,
		SUM(inv_chg_target) - sum(rept_inv_change) as inv_chg_plug,
		sum(cc_inv_impact) - sum(rept_inv_impact) as cc_inv_plug
	FROM calc_difference_to_targets_usa
	WHERE 1=1
	GROUP BY cal_date, market8, pl, customer_engagement
""")

target_plugs_to_fill_gap_usa.createOrReplaceTempView("target_plugs_to_fill_gap_usa")

# COMMAND ----------

target_plugs_to_fill_gap_non_usa = spark.sql("""
SELECT
		cal_date,
		market8,
		pl,
		customer_engagement,
		sum(net_rev_target) - sum(rept_net_rev) as net_rev_plug,
		sum(cc_net_rev_target) - sum(rept_cc_rev) as cc_rev_plug,
		SUM(inv_chg_target) - sum(rept_inv_change) as inv_chg_plug,
		sum(cc_inv_impact) - sum(rept_inv_impact) as cc_inv_plug
	FROM calc_difference_to_targets_non_usa
	WHERE 1=1
	GROUP BY cal_date, market8, pl, customer_engagement
""")

target_plugs_to_fill_gap_non_usa.createOrReplaceTempView("target_plugs_to_fill_gap_non_usa")

# COMMAND ----------

ar_revenue_plugs_mix_calc_usa = spark.sql("""
SELECT 
		cal_date,
		country_alpha2,
		market8,
		region_5,
		platform_subset,
		base_product_number,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		CASE
			WHEN sum(net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
			ELSE net_revenue / sum(net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
		END AS rept_rev_mix,
		sum(currency_impact) as currency_impact,
		sum(cc_net_revenue) as cc_net_revenue,
		CASE
			WHEN sum(cc_net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
			ELSE cc_net_revenue / sum(cc_net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
		END AS rept_cc_rev_mix
  FROM fully_baked_sku_level_data
  WHERE 1=1
	AND net_Revenue > 0
	AND base_product_number NOT IN ('EST_MPS_REVENUE_JV', 'CISS')
    AND country_alpha2 = 'US'
  GROUP BY	cal_date,
		country_alpha2,
		market8,
		region_5,
		platform_subset,
		base_product_number,
		pl,
		customer_engagement,
		net_revenue,
		cc_net_revenue
""")

ar_revenue_plugs_mix_calc_usa.createOrReplaceTempView("ar_revenue_plugs_mix_calc_usa")

# COMMAND ----------

ar_revenue_plugs_mix_calc_non_usa = spark.sql("""
SELECT 
		cal_date,
		country_alpha2,
		market8,
		region_5,
		platform_subset,
		base_product_number,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		CASE
			WHEN sum(net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
			ELSE net_revenue / sum(net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
		END AS rept_rev_mix,
		sum(currency_impact) as currency_impact,
		sum(cc_net_revenue) as cc_net_revenue,
		CASE
			WHEN sum(cc_net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
			ELSE cc_net_revenue / sum(cc_net_revenue) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
		END AS rept_cc_rev_mix
  FROM fully_baked_sku_level_data
  WHERE 1=1
	AND net_Revenue > 0
	AND base_product_number NOT IN ('EST_MPS_REVENUE_JV', 'CISS')
    AND country_alpha2 <> 'US'
  GROUP BY	cal_date,
		country_alpha2,
		market8,
		region_5,
		platform_subset,
		base_product_number,
		pl,
		customer_engagement,
		net_revenue,
		cc_net_revenue
""")

ar_revenue_plugs_mix_calc_non_usa.createOrReplaceTempView("ar_revenue_plugs_mix_calc_non_usa")

# COMMAND ----------

spread_revenue_related_plugs_to_skus_usa = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		COALESCE(sum(net_rev_plug * rept_rev_mix), 1) as net_revenue,
		0 as revenue_units,
		0 as equivalent_units,
		0 as yield_x_units,
		0 as yield_x_units_black_only,
		COALESCE(sum(cc_rev_plug * rept_cc_rev_mix), 1) as cc_net_revenue
  FROM ar_revenue_plugs_mix_calc_usa mix
  JOIN target_plugs_to_fill_gap_usa plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market8 = plugs.market8 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement
""")

spread_revenue_related_plugs_to_skus_usa.createOrReplaceTempView("spread_revenue_related_plugs_to_skus_usa")

# COMMAND ----------

spread_revenue_related_plugs_to_skus_non_usa = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		COALESCE(sum(net_rev_plug * rept_rev_mix), 1) as net_revenue,
		0 as revenue_units,
		0 as equivalent_units,
		0 as yield_x_units,
		0 as yield_x_units_black_only,
		COALESCE(sum(cc_rev_plug * rept_cc_rev_mix), 1) as cc_net_revenue
  FROM ar_revenue_plugs_mix_calc_non_usa mix
  JOIN target_plugs_to_fill_gap_non_usa plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market8 = plugs.market8 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement
""")

spread_revenue_related_plugs_to_skus_non_usa.createOrReplaceTempView("spread_revenue_related_plugs_to_skus_non_usa")

# COMMAND ----------

union_fully_baked_sku_data_with_revenue_gap = spark.sql("""
	SELECT cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,'SYSTEM_ORIGINATED_DATA' as process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
    FROM fully_baked_sku_level_data
    WHERE 1=1
    GROUP BY cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	UNION ALL

	SELECT cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,'REV_ALLOCATION' as process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(cc_net_revenue) - sum(net_revenue) as currency_impact
	  ,sum(net_revenue) as cc_net_revenue
	  ,0 as ci_usd
	  ,0 as ci_qty
	  ,0 as inventory_change_impact
	  ,0 as cc_inventory_impact
	  ,sum(net_revenue) as adjusted_revenue
    FROM spread_revenue_related_plugs_to_skus_usa
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      
  	  UNION ALL

    SELECT cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,'REV_ALLOCATION' as process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(cc_net_revenue) - sum(net_revenue) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,0 as ci_usd
	  ,0 as ci_qty
	  ,0 as inventory_change_impact
	  ,0 as cc_inventory_impact
	  ,sum(cc_net_revenue) as adjusted_revenue
    FROM spread_revenue_related_plugs_to_skus_non_usa
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")
                                                        
union_fully_baked_sku_data_with_revenue_gap.createOrReplaceTempView("union_fully_baked_sku_data_with_revenue_gap")

# COMMAND ----------

fully_baked_with_revenue_and_cc_rev_gaps_spread = spark.sql("""
SELECT cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,process_detail
      ,COALESCE(sum(net_revenue), 0) as net_revenue
      ,COALESCE(sum(revenue_units), 0) as revenue_units
      ,COALESCE(sum(equivalent_units), 0) as equivalent_units
      ,COALESCE(sum(yield_x_units), 0) as yield_x_units
      ,COALESCE(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
      ,COALESCE(sum(currency_impact), 0) as currency_impact
      ,COALESCE(sum(cc_net_revenue), 0) as cc_net_revenue
      ,COALESCE(sum(ci_usd), 0) as ci_usd
      ,COALESCE(sum(ci_qty), 0) as ci_qty
      ,COALESCE(sum(inventory_change_impact), 0) as inventory_change_impact
      ,COALESCE(sum(cc_inventory_impact), 0) as cc_inventory_impact
      ,COALESCE(sum(adjusted_revenue), 0) as adjusted_revenue
    FROM union_fully_baked_sku_data_with_revenue_gap
    WHERE 1=1    
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,process_detail
""")
 
fully_baked_with_revenue_and_cc_rev_gaps_spread.createOrReplaceTempView("fully_baked_with_revenue_and_cc_rev_gaps_spread")

# COMMAND ----------

# JUMP FORWARD TO END, Cmd 126.
# Write out to its delta table target.
fully_baked_with_revenue_and_cc_rev_gaps_spread.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/fully_baked_with_revenue_and_cc_rev_gaps_spread_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.fully_baked_with_revenue_and_cc_rev_gaps_spread_temp USING DELTA LOCATION '/tmp/delta/fin_stage/fully_baked_with_revenue_and_cc_rev_gaps_spread_temp'")

# COMMAND ----------

epa_07_spread_rev_cc_rev = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    ,sum(net_revenue) as net_revenue
    ,sum(revenue_units) as revenue_units
    ,sum(equivalent_units) as equivalent_units
    ,sum(yield_x_units) as yield_x_units
    ,sum(yield_x_units_black_only) as yield_x_units_black_only
    ,sum(currency_impact) as currency_impact
	,sum(cc_net_revenue) as cc_net_revenue
	,sum(ci_usd) as ci_usd
	,sum(ci_qty) as ci_qty
	,sum(inventory_change_impact) as inventory_change_impact
	,sum(cc_inventory_impact) as cc_inventory_impact
	,sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.fully_baked_with_revenue_and_cc_rev_gaps_spread_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_07_spread_rev_cc_rev", epa_07_spread_rev_cc_rev, "epa_07_spread_rev_cc_rev"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Twelve
# MAGIC Spread Channel Inventory Gaps to where we have data  

# COMMAND ----------

original_fully_baked = spark.sql("""
SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM fully_baked_with_revenue_and_cc_rev_gaps_spread
    WHERE 1=1
		AND process_detail = 'SYSTEM_ORIGINATED_DATA' -- use original data (before rev and cc rev spread process)
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
""")

original_fully_baked.createOrReplaceTempView("original_fully_baked")

# COMMAND ----------

# Write out to its delta table target.
original_fully_baked.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/original_fully_baked_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.original_fully_baked_temp USING DELTA LOCATION '/tmp/delta/fin_stage/original_fully_baked_temp'")

# COMMAND ----------

ci_original_data_positive = spark.sql("""
SELECT
		cal_date,
		market8,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM original_fully_baked
	WHERE 1=1
	AND cc_inventory_impact > 0
	GROUP BY cal_date, market8, pl, customer_engagement
""")

ci_original_data_positive.createOrReplaceTempView("ci_original_data_positive")

# COMMAND ----------

ci_original_data_negative = spark.sql("""
SELECT
		cal_date,
		market8,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM original_fully_baked
	WHERE 1=1
	AND cc_inventory_impact < 0
	GROUP BY cal_date, market8, pl, customer_engagement
""")

ci_original_data_negative.createOrReplaceTempView("ci_original_data_negative")

# COMMAND ----------

positive_ci_change = spark.sql("""
SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,CASE
		WHEN sum(inventory_change_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
		ELSE inventory_change_impact / sum(inventory_change_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
	  END AS ci_chg_mix
	  ,CASE
		WHEN sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
		ELSE cc_inventory_impact / sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
	  END AS cc_ci_chg_mix
	FROM original_fully_baked
    WHERE 1=1
		AND cc_inventory_impact > 0
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,inventory_change_impact
	  ,cc_inventory_impact
""")

positive_ci_change.createOrReplaceTempView("positive_ci_change")

# COMMAND ----------

# Write out to its delta table target.
positive_ci_change.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/positive_ci_change_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.positive_ci_change_temp USING DELTA LOCATION '/tmp/delta/fin_stage/positive_ci_change_temp'")

# COMMAND ----------

epa_08_ci_data_positive = """
SELECT
    fiscal_year_qtr,
    fiscal_yr,
	market8,
    region_5,
    pl,
	SUM(inventory_change_impact) as inventory_change_impact,
	sum(cc_inventory_impact) as cc_inventory_impact
FROM fin_stage.positive_ci_change_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_08_ci_data_positive", epa_08_ci_data_positive, "epa_08_ci_data_positive"])

# COMMAND ----------

negative_ci_change = spark.sql("""
SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,CASE
		WHEN sum(inventory_change_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
		ELSE inventory_change_impact / sum(inventory_change_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
	  END AS ci_chg_mix
	  ,CASE
		WHEN sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement) = 0 THEN NULL
		ELSE cc_inventory_impact / sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market8, pl, customer_engagement)
	  END AS cc_ci_chg_mix
	FROM original_fully_baked
    WHERE 1=1
		AND cc_inventory_impact < 0
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,inventory_change_impact
	  ,cc_inventory_impact	
""")

negative_ci_change.createOrReplaceTempView("negative_ci_change")

# COMMAND ----------

# Write out to its delta table target.
negative_ci_change.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/negative_ci_change_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.negative_ci_change_temp USING DELTA LOCATION '/tmp/delta/fin_stage/negative_ci_change_temp'")

# COMMAND ----------

epa_09_ci_data_negative = """
SELECT
    fiscal_year_qtr,
    fiscal_yr,
	market8,
    region_5,
    pl,
	SUM(inventory_change_impact) as inventory_change_impact,
	sum(cc_inventory_impact) as cc_inventory_impact
FROM fin_stage.negative_ci_change_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_09_ci_data_negative", epa_09_ci_data_negative, "epa_09_ci_data_negative"])

# COMMAND ----------

ci_positive_totals = spark.sql("""
SELECT
		cal_date,
		market8,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM target_mkt8_data
	WHERE 1=1
	AND cc_inventory_impact > 0
	GROUP BY cal_date, market8, pl, customer_engagement
""")

ci_positive_totals.createOrReplaceTempView("ci_positive_totals")

# COMMAND ----------

ci_negative_totals = spark.sql("""
SELECT
		cal_date,
		market8,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM target_mkt8_data
	WHERE 1=1
	AND cc_inventory_impact < 0
	GROUP BY cal_date, market8, pl, customer_engagement
""")

ci_negative_totals.createOrReplaceTempView("ci_negative_totals")

# COMMAND ----------

ci_positive_gap = spark.sql("""
SELECT
		gold.cal_date,
		gold.market8,
		gold.pl,
		gold.customer_engagement,
		COALESCE(sum(total.inventory_change_impact), 0) as inventory_change_target,
		COALESCE(sum(total.cc_inventory_impact), 0) as cc_inventory_target,
		COALESCE(sum(gold.inventory_change_impact), 0) as inventory_change_gold,
		COALESCE(sum(gold.cc_inventory_impact), 0) as cc_inventory_gold,
		COALESCE(sum(total.inventory_change_impact), 0) - COALESCE(sum(gold.inventory_change_impact), 0) as inventory_change_plug,
		COALESCE(sum(total.cc_inventory_impact), 0) - COALESCE(sum(gold.cc_inventory_impact), 0) as cc_inventory_plug
	FROM ci_original_data_positive gold
	LEFT JOIN  ci_positive_totals total ON
		total.cal_date = gold.cal_date AND
		total.market8 = gold.market8 AND
		total.pl = gold.pl AND
		total.customer_engagement = gold.customer_engagement
	WHERE 1=1
	GROUP BY gold.cal_date,
		gold.market8,
		gold.pl,
		gold.customer_engagement
""")

ci_positive_gap.createOrReplaceTempView("ci_positive_gap")

# COMMAND ----------

ci_negative_gap = spark.sql("""
SELECT
		gold.cal_date,
		gold.market8,
		gold.pl,
		gold.customer_engagement,
		COALESCE(sum(total.inventory_change_impact), 0) as inventory_change_target,
		COALESCE(sum(total.cc_inventory_impact), 0) as cc_inventory_target,
		COALESCE(sum(gold.inventory_change_impact), 0) as inventory_change_gold,
		COALESCE(sum(gold.cc_inventory_impact), 0) as cc_inventory_gold,
		COALESCE(sum(total.inventory_change_impact), 0) - COALESCE(sum(gold.inventory_change_impact), 0) as inventory_change_plug,
		COALESCE(sum(total.cc_inventory_impact), 0) - COALESCE(sum(gold.cc_inventory_impact), 0) as cc_inventory_plug
	FROM ci_original_data_negative gold
	LEFT JOIN ci_negative_totals total ON
		total.cal_date = gold.cal_date AND
		total.market8 = gold.market8 AND
		total.pl = gold.pl AND
		total.customer_engagement = gold.customer_engagement
	WHERE 1=1
	GROUP BY gold.cal_date,
		gold.market8,
		gold.pl,
		gold.customer_engagement
""")

ci_negative_gap.createOrReplaceTempView("ci_negative_gap")

# COMMAND ----------

ci_positive_plug = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		'CI_ALLOCATION' as process_detail,
		0 as net_revenue,
		0 as revenue_units,
		0 as equivalent_units,
		0 as yield_x_units,
		0 as yield_x_units_black_only,
        0 as currency_impact,
		0 as cc_net_revenue,
		0 as ci_usd,
		0 as ci_qty,
		COALESCE(sum(inventory_change_plug * ci_chg_mix), 0) as inventory_change_impact,
		COALESCE(sum(cc_inventory_plug * cc_ci_chg_mix), 0) as cc_inventory_impact,
		-1 * COALESCE(sum(cc_inventory_plug * cc_ci_chg_mix), 0) as adjusted_revenue
  FROM ci_positive_gap mix 
  JOIN positive_ci_change plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market8 = plugs.market8 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement
""")

ci_positive_plug.createOrReplaceTempView("ci_positive_plug")

# COMMAND ----------

# Write out to its delta table target.
ci_positive_plug.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/ci_positive_plug_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.ci_positive_plug_temp USING DELTA LOCATION '/tmp/delta/fin_stage/ci_positive_plug_temp'")

# COMMAND ----------

epa_10_ci_data_positive_plug = """
SELECT
    fiscal_year_qtr,
    fiscal_yr,
	market8,
    region_5,
    pl,
	SUM(inventory_change_impact) as inventory_change_impact,
	sum(cc_inventory_impact) as cc_inventory_impact
FROM fin_stage.ci_positive_plug_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_10_ci_data_positive_plug", epa_10_ci_data_positive_plug, "epa_10_ci_data_positive_plug"])

# COMMAND ----------

ci_negative_plug = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		'CI_ALLOCATION' as process_detail,
		0 as net_revenue,
		0 as revenue_units,
		0 as equivalent_units,
		0 as yield_x_units,
		0 as yield_x_units_black_only,
        0 as currency_impact,
		0 as cc_net_revenue,
		0 as ci_usd,
		0 as ci_qty,
		COALESCE(sum(inventory_change_plug * ci_chg_mix), 0) as inventory_change_impact,
		COALESCE(sum(cc_inventory_plug * cc_ci_chg_mix), 0) as cc_inventory_impact,
		-1 * COALESCE(sum(cc_inventory_plug * cc_ci_chg_mix), 0) as adjusted_revenue
  FROM ci_negative_gap mix 
  JOIN negative_ci_change plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market8 = plugs.market8 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market8,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement
""")

ci_negative_plug.createOrReplaceTempView("ci_negative_plug")

# COMMAND ----------

# Write out to its delta table target.
ci_negative_plug.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/ci_negative_plug_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.ci_negative_plug_temp USING DELTA LOCATION '/tmp/delta/fin_stage/ci_negative_plug_temp'")

# COMMAND ----------

epa_11_ci_data_negative_plug = """
SELECT
    fiscal_year_qtr,
    fiscal_yr,
	market8,
    region_5,
    pl,
	SUM(inventory_change_impact) as inventory_change_impact,
	sum(cc_inventory_impact) as cc_inventory_impact
FROM fin_stage.ci_negative_plug_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_11_ci_data_negative_plug", epa_11_ci_data_negative_plug, "epa_11_ci_data_negative_plug"])

# COMMAND ----------

old_adjusted_revenue_mash1 = spark.sql("""
	SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM fully_baked_with_revenue_and_cc_rev_gaps_spread
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail

	  UNION ALL

	SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM ci_positive_plug
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail

	UNION ALL
	  
	SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM ci_negative_plug
    WHERE 1=1	
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
""")

old_adjusted_revenue_mash1.createOrReplaceTempView("old_adjusted_revenue_mash1")
#adjusted_revenue_mash1.createOrReplaceTempView("adjusted_revenue_mash1")

# COMMAND ----------

adjusted_revenue_mash1 = spark.sql("""
	SELECT 
       cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM fully_baked_with_revenue_and_cc_rev_gaps_spread
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
""")

adjusted_revenue_mash1.createOrReplaceTempView("adjusted_revenue_mash1")

# COMMAND ----------

# Write out to its delta table target.
adjusted_revenue_mash1.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adjusted_revenue_mash1_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adjusted_revenue_mash1_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adjusted_revenue_mash1_temp'")

# COMMAND ----------

adjusted_revenue_mash2 = spark.sql("""
SELECT 
     cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,COALESCE(sum(net_revenue), 0) as net_revenue
      ,COALESCE(sum(revenue_units), 0) as revenue_units
      ,COALESCE(sum(equivalent_units), 0) as equivalent_units
      ,COALESCE(sum(yield_x_units),0) as yield_x_units
      ,COALESCE(sum(yield_x_units_black_only),0) as yield_x_units_black_only
      ,COALESCE(sum(cc_net_revenue) - sum(net_revenue), 0) as currency_impact
	  ,COALESCE(sum(cc_net_revenue), 0) as cc_net_revenue
	  ,COALESCE(sum(ci_usd), 0) as ci_usd
	  ,COALESCE(sum(ci_qty), 0) as ci_qty
	  ,COALESCE(sum(inventory_change_impact), 0) as inventory_change_impact
      ,COALESCE(sum(cc_inventory_impact) - sum(inventory_change_impact), 0) as constant_currency_inventory_change_impact
	  ,COALESCE(sum(cc_inventory_impact), 0) as cc_inventory_impact
	  ,COALESCE(sum(adjusted_revenue), 0) as adjusted_revenue
      ,COALESCE(sum(ci_usd)/NULLIF(sum(ci_qty), 0), 0) as implied_ndp
      ,COALESCE(sum(yield_x_units)/NULLIF(sum(revenue_units), 0), 0) as implied_yield
      ,COALESCE(sum(yield_x_units_black_only)/NULLIF(sum(revenue_units), 0), 0) as implied_yield_black_only
    FROM adjusted_revenue_mash1
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
""")

adjusted_revenue_mash2.createOrReplaceTempView("adjusted_revenue_mash2")

# COMMAND ----------

adjusted_revenue_mash3 = spark.sql("""
SELECT 
     cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
      ,sum(constant_currency_inventory_change_impact) as constant_currency_inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
      ,sum(implied_ndp) as implied_ndp
      ,sum(implied_yield) as implied_yield
      ,sum(implied_yield_black_only) as implied_yield_black_only
      ,COALESCE(sum(inventory_change_impact)/NULLIF(sum(implied_ndp), 0) * sum(implied_yield), 0) as ci_yield_x_units
      ,COALESCE(sum(inventory_change_impact)/NULLIF(sum(implied_ndp), 0) * sum(implied_yield_black_only), 0) as ci_yield_x_units_black_only
    FROM adjusted_revenue_mash2
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
""")

adjusted_revenue_mash3.createOrReplaceTempView("adjusted_revenue_mash3")

# COMMAND ----------

mps_jv = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,sum(net_revenue) as net_revenue
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM adjusted_revenue_mash3
    WHERE 1=1
		AND base_product_number = 'EST_MPS_REVENUE_JV'
    GROUP BY  cal_date
      ,country_alpha2
""")

mps_jv.createOrReplaceTempView("mps_jv")

# COMMAND ----------

adjusted_revenue_mash4 = spark.sql("""
SELECT 
     cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
      ,COALESCE(sum(net_revenue)/NULLIF(sum(revenue_units), 0), 0) as aru
      ,COALESCE(sum(net_revenue)/NULLIF(sum(equivalent_units), 0), 0) as equivalent_aru
      ,sum(implied_yield) as implied_ink_yield
      ,sum(implied_yield_black_only) as implied_toner_yield
      ,sum(currency_impact) as currency_impact
	  ,sum(net_revenue) + sum(currency_impact) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
      ,sum(constant_currency_inventory_change_impact) as currency_impact_ch_inventory
	  ,sum(inventory_change_impact) + sum(constant_currency_inventory_change_impact) as cc_inventory_impact
	  ,sum(cc_net_revenue) - sum(cc_inventory_impact) as adjusted_revenue
      ,sum(implied_ndp) as implied_ci_ndp
      ,COALESCE(sum(inventory_change_impact)/NULLIF(sum(implied_ndp), 0), 0) as ci_unit_change
      ,COALESCE(sum(inventory_change_impact)/NULLIF(sum(implied_ndp), 0) * sum(implied_yield), 0) as ci_yield_x_units
      ,COALESCE(sum(inventory_change_impact)/NULLIF(sum(implied_ndp), 0) * sum(implied_yield_black_only), 0) as ci_yield_x_units_black_only
    FROM adjusted_revenue_mash3
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
""")

adjusted_revenue_mash4.createOrReplaceTempView("adjusted_revenue_mash4")

# COMMAND ----------

# Write out to its delta table target.
adjusted_revenue_mash4.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/tmp/delta/fin_stage/adjusted_revenue_mash4_temp")

# Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS fin_stage.adjusted_revenue_mash4_temp USING DELTA LOCATION '/tmp/delta/fin_stage/adjusted_revenue_mash4_temp'")

# COMMAND ----------

epa_12_adj_rev_mash4 = """
SELECT fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl
    ,sum(net_revenue) as net_revenue
    ,sum(revenue_units) as revenue_units
    ,sum(equivalent_units) as equivalent_units
    ,sum(yield_x_units) as yield_x_units
    ,sum(yield_x_units_black_only) as yield_x_units_black_only
    ,sum(currency_impact) as currency_impact
	,sum(cc_net_revenue) as cc_net_revenue
	,sum(ci_usd) as ci_usd
	,sum(ci_qty) as ci_qty
	,sum(inventory_change_impact) as inventory_change_impact
	,sum(cc_inventory_impact) as cc_inventory_impact
	,sum(adjusted_revenue) as adjusted_revenue
FROM fin_stage.adjusted_revenue_mash4_temp fa
LEFT JOIN mdm.calendar cal
    ON cal.Date = fa.cal_date
WHERE 1=1
AND Day_of_Month = 1
AND Fiscal_Yr > '2017'
GROUP BY fiscal_year_qtr
    , fiscal_yr
    , market8
    , region_5
    , pl    
"""

query_list.append(["scen.epa_12_adj_rev_mash4", epa_12_adj_rev_mash4, "epa_12_adj_rev_mash4"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adjusted Revenue EPA Staging

# COMMAND ----------

adj_rev_staging = spark.sql("""
SELECT 'ACTUALS - ADJUSTED_REVENUE_EPA' as record
      ,cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield 
	  ,implied_toner_yield
	  ,sum(currency_impact) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd 
	  ,sum(ci_qty) as ci_qty 
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(currency_impact_ch_inventory) as currency_impact_ch_inventory
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp 
	  ,case
		when base_product_number = 'CISS' THEN 1 ELSE 0
	  end as is_host
	  ,sum(ci_unit_change) as monthly_ci_unit_change
	  ,sum(ci_yield_x_units) as ci_ccs_pages_change
	  ,sum(ci_yield_x_units_black_only) as ci_ccs_pages_blk_only_change
	  ,1 as official
	  ,(select load_date from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE_EPA' and
		load_date = (select max(load_date) from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE_EPA')) as load_date
      ,(select version from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE_EPA' and
		version = (select max(version) from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE_EPA')) as version
	FROM adjusted_revenue_mash4
    WHERE 1=1	
    GROUP BY  cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,implied_ci_ndp
	  ,process_detail
""")

adj_rev_staging.createOrReplaceTempView("adjusted_revenue_epa_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue EPA Promotion

# COMMAND ----------

adj_rev_epa = spark.sql("""
SELECT record
      ,cal_date
      ,country_alpha2
      ,market8 as GEOGRAPHY
	  ,'MARKET8' as geography_grain
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail--new
      ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units--new
      ,sum(equivalent_units) as equivalent_units--new
      ,sum(yield_x_units) as yield_x_units--new
      ,sum(yield_x_units_black_only) as yield_x_units_black_only--new
	  ,aru --new
	  ,equivalent_aru --new
	  ,implied_ink_yield -- new
	  ,implied_toner_yield --new
	  ,sum(cc_net_revenue) - sum(net_revenue) as currency_impact
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd --new
	  ,sum(ci_qty) as ci_qty --new
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) - sum(inventory_change_impact) as currency_impact_ch_inventory
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp --new
	  ,is_host
	  ,sum(monthly_ci_unit_change) as monthly_ci_unit_change
	  ,sum(ci_ccs_pages_change) as ci_ccs_pages_change
	  ,sum(ci_ccs_pages_blk_only_change) as ci_ccs_pages_blk_only_change
	  ,1 as official
	  ,load_date
      ,version
	FROM adjusted_revenue_epa_staging
    WHERE 1=1	
    GROUP BY  record
	  ,cal_date
      ,country_alpha2
      ,market8
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,implied_ci_ndp
	  ,process_detail
	  ,load_date
	  ,version
	  ,is_host
""")

write_df_to_redshift(configs, adj_rev_epa, "fin_prod.adjusted_revenue_epa", "append")

# COMMAND ----------

#grant team access
query_access_grant = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_epa TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query_access_grant)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Driver Output

# COMMAND ----------

adj_rev_salesprod_dropped = spark.sql("""
	SELECT		
       cal_date
      ,country_alpha2
      ,market8 as market10
	  ,region_5
      ,sales_product_number
      ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM adjusted_revenue_negative
    WHERE 1=1	
    GROUP BY   cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,sales_product_number
      ,pl
      ,customer_engagement
""")

write_df_to_redshift(configs, adj_rev_salesprod_dropped, "fin_prod.adjusted_revenue_epa_adjrev_salesprod_dropped_data", "overwrite")

# COMMAND ----------

query_access_grant = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_epa_adjrev_salesprod_dropped_data TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query_access_grant)

# COMMAND ----------

epa_sales_to_base_conversion = spark.sql("""
	SELECT		
       cal_date
      ,country_alpha2
      ,market8 as market10
	  ,region_5
      ,sales_product_number
      ,sales_product_line_code as pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM salesprod_missing_baseprod
    WHERE 1=1	
    GROUP BY   cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,sales_product_number
      ,sales_product_line_code
      ,customer_engagement
""")

write_df_to_redshift(configs, epa_sales_to_base_conversion, "fin_prod.adjusted_revenue_epa_sales_to_base_conversion_unknowns", "overwrite")

# COMMAND ----------

query_access_grant = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_epa_sales_to_base_conversion_unknowns TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query_access_grant)

# COMMAND ----------

epa_actuals_baseprod_negative_net_revenue = spark.sql("""
	SELECT		
       cal_date
      ,country_alpha2
      ,market8 as market10
	  ,region_5
      ,base_product_number
	  ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
	  ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	FROM baseprod_detailed_targets
    WHERE 1=1
		AND net_revenue < 0
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,base_product_number
      ,pl
      ,customer_engagement
""")

write_df_to_redshift(configs, epa_actuals_baseprod_negative_net_revenue, "fin_prod.adjusted_revenue_epa_actuals_baseprod_negative_net_revenue", "overwrite")

# COMMAND ----------

query_access_grant = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_epa_actuals_baseprod_negative_net_revenue TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query_access_grant)

# COMMAND ----------

epa_actuals_baseprod_dropped_data = spark.sql("""
	SELECT		
       cal_date
      ,country_alpha2
      ,market8 as market10
	  ,region_5
      ,base_product_number
	  ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
	  ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	FROM financials_dropped_out
    WHERE 1=1
		AND net_revenue <= 0
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,base_product_number
      ,pl
      ,customer_engagement
""")

write_df_to_redshift(configs, epa_actuals_baseprod_dropped_data, "fin_prod.adjusted_revenue_epa_actuals_baseprod_dropped_data", "overwrite")

# COMMAND ----------

query_access_grant = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_epa_actuals_baseprod_dropped_data TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query_access_grant)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Test Tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
