# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue EPA

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

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
    .option("dbtable", "fin_prod.adjusted_revenue_salesprod") \
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

tables = [
    ['prod.version', version_df, "overwrite"], 
    ['fin_prod.actuals_supplies_baseprod', actuals_supplies_baseprod, "overwrite"],
    ['fin_prod.adjusted_revenue_salesprod', adjusted_revenue_salesprod, "overwrite"],
    ['mdm.calendar', calendar, "overwrite"],
    ['mdm.iso_country_code_xref', iso_country_code_xref, "overwrite"],
    ['mdm.product_line_xref', product_line_xref, "overwrite"],
    ['mdm.rdma_base_to_sales_product_map', rdma_base_to_sales_product_map, "overwrite"]
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
		geography as market10,
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

adj_rev_cleaned = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market10,
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
		market10,
		region_5,
		sales_product_number,
		pl,
		customer_engagement

""")

adj_rev_cleaned.createOrReplaceTempView("adjusted_revenue_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Two
# MAGIC Aggregate ARS data to determine "official targets" (excluding where finance does not restate)

# COMMAND ----------

two_yrs = spark.sql("""
SELECT 
	cast(Fiscal_Yr-2 as int) as Start_Fiscal_Yr
FROM mdm.calendar
WHERE Date = (select current_date())
""")

two_yrs.createOrReplaceTempView("two_years")

# COMMAND ----------

fin_adj_rev_targets_without_all_edw = spark.sql("""
WITH 

official_salesprod_targets AS
(
	SELECT 
      cal_date
      ,market10
      ,pl
      ,customer_engagement
	  ,SUM(net_revenue) AS net_revenue
	  ,SUM(cc_net_revenue) AS cc_net_revenue
	  ,SUM(inventory_change_impact) AS inventory_change_impact
	  ,SUM(cc_inventory_impact) AS cc_inventory_impact
	  ,SUM(adjusted_revenue) AS adjusted_revenue
  FROM adjusted_revenue_finance
  WHERE 1=1
	AND sales_product_number <> 'EDW_TIE_TO_PLANET'
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,market10
      ,pl
      ,customer_engagement

	UNION ALL

	SELECT 
      cal_date
      ,market10
      ,pl
      ,customer_engagement
	  ,SUM(net_revenue) AS net_revenue
	  ,SUM(cc_net_revenue) AS cc_net_revenue
	  ,SUM(inventory_change_impact) AS inventory_change_impact
	  ,SUM(cc_inventory_impact) AS cc_inventory_impact
	  ,SUM(adjusted_revenue) AS adjusted_revenue
  FROM adjusted_revenue_finance ar
  LEFT JOIN mdm.calendar cal ON cal.Date = ar.cal_date
  WHERE 1=1
	AND Day_of_month = 1
	AND sales_product_number IN ('EDW_TIE_TO_PLANET')
	AND Fiscal_Yr >= (SELECT Start_Fiscal_Yr FROM two_years)
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,market10
      ,pl
      ,customer_engagement

    UNION ALL

	SELECT 
      cal_date
      ,market10
      ,pl
      ,customer_engagement
	  ,SUM(net_revenue) AS net_revenue
	  ,SUM(cc_net_revenue) AS cc_net_revenue
	  ,SUM(inventory_change_impact) AS inventory_change_impact
	  ,SUM(cc_inventory_impact) AS cc_inventory_impact
	  ,SUM(adjusted_revenue) AS adjusted_revenue
  FROM adjusted_revenue_finance ar
  WHERE 1=1
	AND pl = 'GD'
  GROUP BY  cal_date
      ,market10
      ,pl
      ,customer_engagement
)

	SELECT 
      cal_date
      ,market10
      ,pl
      ,customer_engagement
	  ,IFNULL(SUM(net_revenue), 0) AS net_revenue
	  ,IFNULL(SUM(cc_net_revenue), 0) AS cc_net_revenue
	  ,IFNULL(SUM(inventory_change_impact), 0) AS inventory_change_impact
	  ,IFNULL(SUM(cc_inventory_impact), 0) AS cc_inventory_impact
	  ,IFNULL(SUM(adjusted_revenue), 0) AS adjusted_revenue
  FROM official_salesprod_targets 
  WHERE 1=1
  GROUP BY  cal_date
      ,market10
      ,pl
      ,customer_engagement
""")

fin_adj_rev_targets_without_all_edw.createOrReplaceTempView("finance_adj_rev_targets_without_all_edw")

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
		market10,
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
		market10,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
""")

adj_rev_positive.createOrReplaceTempView("adjusted_revenue_positive")

# COMMAND ----------

adj_rev_negative = spark.sql("""
SELECT
		cal_date,
		country_alpha2,
		market10,
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
		OR sales_product_number = 'EDW_tie_to_planet'
	    OR sales_product_number LIKE 'UNK%'
		OR sales_product_number LIKE '%proxy%'
		OR sales_product_number IN ('Birds', 'CTSS', 'CISS', 'est_mps_revenue_jv')
	GROUP BY cal_date,
		country_alpha2,
		market10,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
""")

adj_rev_negative.createOrReplaceTempView("adjusted_revenue_negative")

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
		market10,
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
		market10,
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
		market10,
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
	FROM adjusted_revenue_finance
	WHERE 1=1
		AND pl = 'GD'
	GROUP BY cal_date,
		country_alpha2,
		market10,
		region_5,
		--sales_product_number,
		pl,
		customer_engagement		
""")

adj_rev_gd.createOrReplaceTempView("adjusted_revenue_GD")

# COMMAND ----------

iink_unit_mix = spark.sql("""
SELECT 
		cal_date,
		market10,
		sales_product_number,
		pl,
		customer_engagement,
		SUM(revenue_units) as revenue_units,	
		CASE
			WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
			ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
		END AS unit_mix
	FROM fin_prod.actuals_supplies_salesprod 
	WHERE 1=1
	AND pl = 'GD'
	AND revenue_units > 0
	GROUP BY cal_date, pl, customer_engagement, sales_product_number, market10, revenue_units
""")

iink_unit_mix.createOrReplaceTempView("iink_units_mix")

# COMMAND ----------

adj_rev_gd_add_skus =  spark.sql("""
SELECT
		gd.cal_date,
		gd.country_alpha2,
		gd.market10,
		gd.region_5,
		iink.sales_product_number,
		gd.pl,
		gd.customer_engagement,
		IFNULL(SUM(net_revenue * unit_mix), 0) AS net_revenue,
		IFNULL(SUM(currency_impact * unit_mix), 0) AS currency_impact,
		IFNULL(SUM(cc_net_revenue * unit_mix), 0) AS cc_net_revenue,
		IFNULL(SUM(inventory_usd * unit_mix), 0) AS inventory_usd,
		IFNULL(SUM(prev_inv_usd * unit_mix), 0) AS prev_inv_usd,
		IFNULL(SUM(inventory_qty * unit_mix), 0) AS inventory_qty,
		IFNULL(SUM(prev_inv_qty * unit_mix), 0) AS prev_inv_qty,
		IFNULL(SUM(monthly_unit_change * unit_mix), 0) AS monthly_unit_change,
		IFNULL(SUM(monthly_inv_usd_change * unit_mix), 0) AS monthly_inv_usd_change,
		IFNULL(SUM(inventory_change_impact * unit_mix), 0) AS inventory_change_impact,
		IFNULL(SUM(currency_impact_ch_inventory * unit_mix), 0) AS currency_impact_ch_inventory,
		IFNULL(SUM(cc_inventory_impact * unit_mix), 0) AS cc_inventory_impact,
		IFNULL(SUM(adjusted_revenue * unit_mix), 0) AS adjusted_revenue 
	FROM adjusted_revenue_GD gd
	INNER JOIN iink_units_mix iink ON
		gd.cal_date = iink.cal_date AND
		gd.market10 = iink.market10 AND
		gd.pl = iink.pl AND
		gd.customer_engagement = iink.customer_engagement
	WHERE 1=1
	GROUP BY 
		gd.cal_date,
		gd.country_alpha2,
		gd.market10,
		gd.region_5,
		iink.sales_product_number,
		gd.pl,
		gd.customer_engagement
""")

adj_rev_gd_add_skus.createOrReplaceTempView("adjusted_revenue_GD_add_skus")

# COMMAND ----------

adj_rev_with_iink_sku_pnl = spark.sql("""
WITH
	
	adjusted_revenue_plus_iink_units AS
	(
	SELECT
		cal_date,
		country_alpha2,
		market10,
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
		market10,
		region_5,
		sales_product_number,
		pl,
		customer_engagement

	UNION ALL

	SELECT
		cal_date,
		country_alpha2,
		market10,
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
		market10,
		region_5,
		sales_product_number,
		pl,
		customer_engagement
)

	SELECT
		cal_date,
		country_alpha2,
		market10,
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
		market10,
		region_5,
		sales_product_number,
		pl,
		customer_engagement	
""")

adj_rev_with_iink_sku_pnl.createOrReplaceTempView("adjusted_revenue_with_iink_sku_pnl")

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
            sales_product_line_code,
            base_product_number,
            base_product_line_code,
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
				market10,
				region_5,
				sp.sales_product_number,
				sp.pl AS sales_product_line_code,
				base_product_number,
				base_product_line_code,
				customer_engagement,
				IFNULL(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
				IFNULL(SUM(currency_impact * base_product_amount_percent/100), SUM(currency_impact)) AS currency_impact,
				IFNULL(SUM(cc_net_revenue * base_product_amount_percent/100), SUM(cc_net_revenue)) AS cc_net_revenue,
				IFNULL(SUM(inventory_usd * base_product_amount_percent/100), SUM(inventory_usd)) AS inventory_usd,
				IFNULL(SUM(prev_inv_usd * base_product_amount_percent/100), SUM(prev_inv_usd)) AS prev_inv_usd,
				IFNULL(SUM(inventory_qty * base_product_amount_percent/100), SUM(inventory_qty)) AS inventory_qty,
				IFNULL(SUM(prev_inv_qty * base_product_amount_percent/100), SUM(prev_inv_qty)) AS prev_inv_qty,
				IFNULL(SUM(monthly_unit_change * base_product_amount_percent/100), SUM(monthly_unit_change)) AS monthly_unit_change,
				IFNULL(SUM(monthly_inv_usd_change * base_product_amount_percent/100), SUM(monthly_inv_usd_change)) AS monthly_inv_usd_change,
				IFNULL(SUM(inventory_change_impact * base_product_amount_percent/100), SUM(inventory_change_impact)) AS inventory_change_impact,
				IFNULL(SUM(currency_impact_ch_inventory * base_product_amount_percent/100), SUM(currency_impact_ch_inventory)) AS currency_impact_ch_inventory,
				IFNULL(SUM(cc_inventory_impact * base_product_amount_percent/100), SUM(cc_inventory_impact)) AS cc_inventory_impact,
				IFNULL(SUM(adjusted_revenue * base_product_amount_percent/100), SUM(adjusted_revenue)) AS adjusted_revenue 
			FROM adjusted_revenue_with_iink_sku_pnl AS sp
			INNER JOIN rdma AS r ON 
				sp.sales_product_number = r.sales_product_number
			WHERE 1=1
			GROUP BY cal_date,
				country_alpha2,
				market10,
				region_5,
				sp.sales_product_number,
				sp.pl,
				base_product_number,
				base_product_line_code,
				customer_engagement
""")

adj_rev_baseprod_conversion.createOrReplaceTempView("adjusted_revenue_baseprod_conversion")

# COMMAND ----------

salesprod_minus_baseprod = spark.sql("""
SELECT 
				cal_date,
				country_alpha2,
				market10,
				region_5,
				sp.sales_product_number,
				sp.pl AS sales_product_line_code,
				base_product_number,
				base_product_line_code,
				customer_engagement,
				IFNULL(SUM(net_revenue * base_product_amount_percent/100), SUM(net_revenue)) AS net_revenue,
				IFNULL(SUM(currency_impact * base_product_amount_percent/100), SUM(currency_impact)) AS currency_impact,
				IFNULL(SUM(cc_net_revenue * base_product_amount_percent/100), SUM(cc_net_revenue)) AS cc_net_revenue,
				IFNULL(SUM(inventory_usd * base_product_amount_percent/100), SUM(inventory_usd)) AS inventory_usd,
				IFNULL(SUM(prev_inv_usd * base_product_amount_percent/100), SUM(prev_inv_usd)) AS prev_inv_usd,
				IFNULL(SUM(inventory_qty * base_product_amount_percent/100), SUM(inventory_qty)) AS inventory_qty,
				IFNULL(SUM(prev_inv_qty * base_product_amount_percent/100), SUM(prev_inv_qty)) AS prev_inv_qty,
				IFNULL(SUM(monthly_unit_change * base_product_amount_percent/100), SUM(monthly_unit_change)) AS monthly_unit_change,
				IFNULL(SUM(monthly_inv_usd_change * base_product_amount_percent/100), SUM(monthly_inv_usd_change)) AS monthly_inv_usd_change,
				IFNULL(SUM(inventory_change_impact * base_product_amount_percent/100), SUM(inventory_change_impact)) AS inventory_change_impact,
				IFNULL(SUM(currency_impact_ch_inventory * base_product_amount_percent/100), SUM(currency_impact_ch_inventory)) AS currency_impact_ch_inventory,
				IFNULL(SUM(cc_inventory_impact * base_product_amount_percent/100), SUM(cc_inventory_impact)) AS cc_inventory_impact,
				IFNULL(SUM(adjusted_revenue * base_product_amount_percent/100), SUM(adjusted_revenue)) AS adjusted_revenue 
			FROM adjusted_revenue_with_iink_sku_pnl AS sp
			LEFT JOIN rdma AS r ON 
				sp.sales_product_number = r.sales_product_number
			WHERE 1=1
				AND sp.sales_product_number NOT IN ('BIRDS', 'CISS', 'CTSS', 'EST_MPS_REVENUE_JV', 'EDW_TIE_TO_PLANET')
				AND base_product_number IS NULL
			GROUP BY cal_date,
				country_alpha2,
				market10,
				region_5,
				sp.sales_product_number,
				sp.pl,
				base_product_number,
				base_product_line_code,
				customer_engagement
""")

salesprod_minus_baseprod.createOrReplaceTempView("salesprod_missing_baseprod")

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
				market10,
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
			GROUP BY cal_date, country_alpha2, base_product_line_code, customer_engagement, base_product_number, market10, region_5
		)
				
			SELECT 
				cal_date,
				country_alpha2,
				market10,
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
			GROUP BY cal_date, country_alpha2, base_product_line_code, customer_engagement, base_product_number, market10, region_5
""")

adj_rev_base_product_data.createOrReplaceTempView("adjusted_revenue_base_product_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Seven
# MAGIC Pull in official actuals (ASB) and do mkt 8 clean up

# COMMAND ----------

orig_official_fin_prep = spark.sql("""
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

orig_official_fin_prep.createOrReplaceTempView("original_official_financials_prep")

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
  FROM original_official_financials_prep
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

orig_official_fin_prep2.createOrReplaceTempView("original_official_financials_prep2")

# COMMAND ----------

orig_official_fin = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market8 as market10
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
  FROM original_official_financials_prep2
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market8
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

orig_official_fin.createOrReplaceTempView("original_official_financials")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Eight
# MAGIC Drop edw tieouts that we're excluding (for now, pending REQs) and compute implied yields

# COMMAND ----------

official_fin_excl_edw_plug = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
  FROM original_official_financials fin
  WHERE 1=1
	AND base_product_number NOT IN ('edw_tie_to_planet')
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

official_fin_excl_edw_plug.createOrReplaceTempView("official_financials_excl_edw_plug")

# COMMAND ----------

orig_official_edw_plugs = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
  FROM original_official_financials fin
  LEFT JOIN mdm.calendar cal ON fin.cal_date = cal.Date
  WHERE 1=1
	AND Day_of_month = 1
	AND base_product_number IN ('EDW_TIE_TO_PLANET')
	AND Fiscal_Yr >= (SELECT Start_Fiscal_Yr FROM two_years)
	AND pl <> 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

orig_official_edw_plugs.createOrReplaceTempView("original_official_edw_plugs")

# COMMAND ----------

baseprod_detailed_targets = spark.sql("""
WITH 

official_baseprod_targets AS
(
	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
  FROM official_financials_excl_edw_plug
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
  FROM original_official_edw_plugs
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	  UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	AND pl = 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
)

	SELECT 
      cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
      ,ifnull(sum(net_revenue), 0) as net_revenue
      ,ifnull(sum(revenue_units), 0) as revenue_units
      ,ifnull(sum(equivalent_units), 0) as equivalent_units
      ,ifnull(sum(yield_x_units), 0) as yield_x_units
      ,ifnull(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
  FROM official_baseprod_targets
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

baseprod_detailed_targets.createOrReplaceTempView("baseprod_targets_detailed")

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
      ,market10
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
  FROM baseprod_targets_detailed
  WHERE 1=1
	AND revenue_units >= 0
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
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
      ,market10
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
      ,market10
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
      ,market10
	  ,region_5
      ,pl
      ,customer_engagement
	  ,sum(net_revenue) as net_revenue
      ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
  FROM baseprod_targets_detailed
  WHERE 1=1
	AND pl = 'GD'
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,pl
      ,customer_engagement
""")

baseprod_fin_official_w_gd.createOrReplaceTempView("baseprod_fin_official_w_GD")

# COMMAND ----------

iink_units_mix_base_w_printer = spark.sql("""
SELECT 
		cal_date,
		market10,
		base_product_number,
		platform_subset,
		pl,
		customer_engagement,
		SUM(revenue_units) as revenue_units,	
		CASE
			WHEN SUM(revenue_units) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
			ELSE revenue_units / SUM(revenue_units) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
		END AS unit_mix
	FROM fin_prod.actuals_supplies_baseprod 
	WHERE 1=1
	AND pl = 'GD'
	AND revenue_units > 0
	AND platform_subset <> 'NA'
	GROUP BY cal_date, pl, customer_engagement, base_product_number, market10, revenue_units, platform_subset
""")

iink_units_mix_base_w_printer.createOrReplaceTempView("iink_units_mix_base_w_printer")

# COMMAND ----------

baseprod_fin_gd_add_sku_data = spark.sql("""
SELECT
		gd.cal_date,
		gd.country_alpha2,
		gd.market10,
		gd.region_5,
		iink.base_product_number,
		iink.platform_subset,
		gd.pl,
		gd.customer_engagement,			
		IFNULL(SUM(unit_mix * gd.net_revenue), 0) AS net_revenue,
		IFNULL(SUM(unit_mix * gd.revenue_units), 0) AS revenue_units,
		IFNULL(SUM(equivalent_units * unit_mix), 0) AS equivalent_units,
		IFNULL(SUM(yield_x_units * unit_mix), 0) AS yield_x_units,
		IFNULL(SUM(yield_x_units_black_only * unit_mix), 0) AS yield_x_units_black_only
	FROM baseprod_fin_official_w_GD gd
	INNER JOIN iink_units_mix_base_w_printer iink ON
		gd.cal_date = iink.cal_date AND
		gd.market10 = iink.market10 AND
		gd.pl = iink.pl AND
		gd.customer_engagement = iink.customer_engagement
	WHERE 1=1
	GROUP BY 
		gd.cal_date,
		gd.country_alpha2,
		gd.market10,
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
      ,market10
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
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement

	 UNION ALL
	
	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
)

	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
""")

baseprod_data_gd_unit_adjusted.createOrReplaceTempView("baseprod_data_GD_unit_adjusted")

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
      ,market10
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
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

fin_gold_dataset.createOrReplaceTempView("financials_gold_dataset")

# COMMAND ----------

fin_dropped_out = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	AND net_revenue <= 0
	AND platform_subset = 'NA'
	AND base_product_number LIKE 'UNK%'
	AND base_product_number LIKE 'EDW%'
	AND base_product_number IN ('BIRDS', 'CTSS', 'CISS', 'EST_MPS_REVENUE_JV')  -- will add back later
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

fin_dropped_out.createOrReplaceTempView("financials_dropped_out")

# COMMAND ----------

fin_with_printer_mix_calc = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
      ,market10
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
      ,market10
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
	  ,ifnull(sum(net_revenue) / nullif(sum(revenue_units), 0), 0) as aru
	  ,ifnull(sum(net_revenue) / nullif(sum(equivalent_units), 0), 0) as equivalent_aru
	  ,ifnull(sum(yield_x_units) / nullif(sum(revenue_units), 0), 0) as implied_ink_yield
	  ,ifnull(sum(yield_x_units_black_only) / nullif(sum(revenue_units), 0), 0) as implied_toner_yield
  FROM financials_with_printer_mix_calc
  WHERE 1=1
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,net_revenue
""")

base_financials_arus_yields.createOrReplaceTempView("base_financials_arus_yields")

# COMMAND ----------

prepped_adj_rev_data = spark.sql("""
SELECT 
    cal_date,
    country_alpha2,
    market10,
    region_5,
    base_product_number,
    pl,
    customer_engagement,
    SUM(currency_impact) AS currency_impact,
    SUM(inventory_usd) AS inventory_usd,
    SUM(inventory_qty) AS inventory_qty,
    SUM(inventory_change_impact) AS inventory_change_impact,
    SUM(currency_impact_ch_inventory) AS currency_impact_ch_inventory,
    SUM(cc_inventory_impact) AS cc_inventory_impact,
    SUM(adjusted_revenue) AS adjusted_revenue,
    IFNULL(sum(inventory_usd) / nullif(sum(inventory_qty), 0), 0) as implied_ci_ndp
FROM adjusted_revenue_base_product_data
WHERE 1=1
GROUP BY cal_date, country_alpha2, pl, customer_engagement, base_product_number, market10, region_5
""")

prepped_adj_rev_data.createOrReplaceTempView("prepped_adjrev_data")

# COMMAND ----------

baseprod_fin_with_adj_rev_variables = spark.sql("""
SELECT 
      mix.cal_date
      ,mix.country_alpha2
      ,mix.market10
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
	  ,aru
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,sum(currency_impact * ifnull(fin_rev_mix, 0)) as currency_impact
	  ,sum(inventory_usd * ifnull(fin_rev_mix, 0)) as ci_usd
	  ,sum(inventory_qty * ifnull(fin_rev_mix, 0)) as ci_qty
	  ,sum(inventory_change_impact * ifnull(fin_rev_mix, 0)) as inventory_change_impact
	  ,sum(cc_inventory_impact * ifnull(fin_rev_mix, 0)) as cc_inventory_impact
	  ,sum(adjusted_revenue * ifnull(fin_rev_mix, 0)) as adjusted_revenue
	  ,implied_ci_ndp
  FROM base_financials_arus_yields mix
  LEFT JOIN prepped_adjrev_data cc_rev ON
	mix.cal_date = cc_rev.cal_date AND
	mix.country_alpha2 = cc_rev.country_alpha2 AND
	mix.market10 = cc_rev.market10 AND
	mix.region_5 = cc_rev.region_5 AND
	mix.base_product_number = cc_rev.base_product_number AND
	mix.pl = cc_rev.pl AND
	mix.customer_engagement = cc_rev.customer_engagement
  WHERE 1=1
	AND net_revenue > 0
  GROUP BY   mix.cal_date
      ,mix.country_alpha2
      ,mix.market10
	  ,mix.region_5
      ,platform_subset
      ,mix.base_product_number
      ,mix.pl
      ,mix.customer_engagement
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,implied_ci_ndp
""")

baseprod_fin_with_adj_rev_variables.createOrReplaceTempView("baseprod_fin_with_adjrev_variables")

# COMMAND ----------

fully_baked_sku_level_data = spark.sql("""
with rejoin_special_cases_with_scrubbed_data AS
(
	SELECT 
     cal_date
      ,country_alpha2
      ,market10
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
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,sum(currency_impact) as currency_impact
	  ,sum(net_revenue) + sum(currency_impact) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
    FROM baseprod_fin_with_adjrev_variables
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

	UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,'NA' as platform_subset
      ,sales_product_number as base_product_number
      ,pl
      ,customer_engagement
      ,ifnull(sum(net_revenue), 0) as net_revenue
      ,0 as revenue_units
      ,0 as equivalent_units
      ,0 as yield_x_units
      ,0 as yield_x_units_black_only
	  ,0 as aru
	  ,0 as equivalent_aru
	  ,0 as implied_ink_yield
	  ,0 as implied_toner_yield
	  ,0 as currency_impact
	  ,ifnull(sum(net_revenue),0) as cc_net_revenue
	  ,0 as ci_usd
	  ,0 as ci_qty
	  ,0 as inventory_change_impact
	  ,0 as cc_inventory_impact
	  ,ifnull(sum(net_revenue), 0) as adjusted_revenue
	  ,0 as implied_ci_ndp
  FROM adjusted_revenue_finance
  WHERE 1=1
	AND sales_product_number IN ('CISS', 'EST_MPS_REVENUE_JV')
  GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,sales_product_number
      ,pl
      ,customer_engagement
)

SELECT 
     cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,ifnull(sum(net_revenue), 0) as net_revenue
      ,ifnull(sum(revenue_units), 0) as revenue_units
      ,ifnull(sum(equivalent_units), 0) as equivalent_units
      ,ifnull(sum(yield_x_units), 0) as yield_x_units
      ,ifnull(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,ifnull(sum(currency_impact), 0) as currency_impact
	  ,ifnull(sum(cc_net_revenue), 0) as cc_net_revenue
	  ,ifnull(sum(ci_usd), 0) as ci_usd
	  ,ifnull(sum(ci_qty), 0) as ci_qty
	  ,ifnull(sum(inventory_change_impact), 0) as inventory_change_impact
	  ,ifnull(sum(cc_inventory_impact), 0) as cc_inventory_impact
	  ,ifnull(sum(adjusted_revenue), 0) as adjusted_revenue
	  ,implied_ci_ndp
    FROM rejoin_special_cases_with_scrubbed_data
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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
""")

fully_baked_sku_level_data.createOrReplaceTempView("fully_baked_sku_level_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Eleven
# MAGIC Spread gap in revenue and cc_revenue caused by dropped data to where we have data 

# COMMAND ----------

reported_mkt10_data = spark.sql("""
SELECT cal_date,
		market10,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		sum(cc_net_revenue) as cc_net_revenue,
		sum(inventory_change_impact) as inventory_change_impact,
	    sum(cc_inventory_impact) as cc_inventory_impact,
	    sum(adjusted_revenue) as adjusted_revenue
	FROM fully_baked_sku_level_data
	GROUP BY cal_date, market10, pl, customer_engagement
""")

reported_mkt10_data.createOrReplaceTempView("reported_mkt10_data")

# COMMAND ----------

target_mkt10_data = spark.sql("""
SELECT cal_date,
		market10,
		pl,
		customer_engagement,
		sum(net_revenue) as net_revenue,
		sum(cc_net_revenue) as cc_net_revenue,
		sum(inventory_change_impact) as inventory_change_impact,
	    sum(cc_inventory_impact) as cc_inventory_impact,
	    sum(adjusted_revenue) as adjusted_revenue
	FROM finance_adj_rev_targets_without_all_edw
	WHERE 1=1
	AND pl IN (select distinct pl from reported_mkt10_data)
	AND net_revenue <> 0
	GROUP BY cal_date, market10, pl, customer_engagement
""")

target_mkt10_data.createOrReplaceTempView("target_mkt10_data")

# COMMAND ----------

calc_difference_to_targets = spark.sql("""
SELECT
		r.cal_date,
		r.market10,
		r.pl,
		r.customer_engagement,
		sum(r.net_revenue) as rept_net_rev,
		sum(far.net_revenue) as net_rev_target,
		sum(r.cc_net_Revenue) as rept_cc_rev,
		sum(far.cc_net_revenue) as cc_net_rev_target,
		sum(r.inventory_change_impact) as rept_inv_change,
		sum(far.inventory_change_impact) as inv_chg_target,
		sum(r.cc_inventory_impact) as rept_inv_impact,
		sum(far.cc_inventory_impact) as cc_inv_impact,
		sum(r.adjusted_revenue) as rept_adj_rev,
		sum(far.adjusted_revenue) as adj_rev_target
	FROM reported_mkt10_data r
	LEFT JOIN target_mkt10_data far ON
		r.cal_date = far.cal_date AND
		r.market10 = far.market10 AND
		r.pl = far.pl AND
		r.customer_engagement = far.customer_engagement
	WHERE 1=1
		AND r.net_revenue <> 0
	GROUP BY r.cal_date, r.market10, r.pl, r.customer_engagement

""")

calc_difference_to_targets.createOrReplaceTempView("calc_difference_to_targets")

# COMMAND ----------

target_plugs_to_fill_gap = spark.sql("""
SELECT
		cal_date,
		market10,
		pl,
		customer_engagement,
		sum(net_rev_target) - sum(rept_net_rev) as net_rev_plug,
		sum(cc_net_rev_target) - sum(rept_cc_rev) as cc_rev_plug,
		SUM(inv_chg_target) - sum(rept_inv_change) as inv_chg_plug,
		sum(cc_inv_impact) - sum(rept_inv_impact) as cc_inv_plug,
		sum(adj_rev_target) - sum(rept_adj_rev) as adjrev_plug
	FROM calc_difference_to_targets
	WHERE 1=1
	GROUP BY cal_date, market10, pl, customer_engagement
""")

target_plugs_to_fill_gap.createOrReplaceTempView("target_plugs_to_fill_gap")

# COMMAND ----------

ar_revenue_plugs_mix_calc = spark.sql("""
SELECT 
		cal_date,
		country_alpha2,
		market10,
		region_5,
		platform_subset,
		base_product_number,
		pl,
		customer_engagement,
		aru,
		equivalent_aru,
		implied_ink_yield,
		implied_toner_yield,
		implied_ci_ndp,
		sum(net_revenue) as net_revenue,
		CASE
			WHEN sum(net_revenue) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
			ELSE net_revenue / sum(net_revenue) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
		END AS rept_rev_mix,
		sum(currency_impact) as currency_impact,
		sum(cc_net_revenue) as cc_net_revenue,
		CASE
			WHEN sum(currency_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
			ELSE currency_impact / sum(currency_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
		END AS rept_cc_rev_mix
  FROM fully_baked_sku_level_data
  WHERE 1=1
	AND net_Revenue > 0
	AND base_product_number NOT IN ('EST_MPS_REVENUE_JV', 'CISS')
  GROUP BY	cal_date,
		country_alpha2,
		market10,
		region_5,
		platform_subset,
		base_product_number,
		pl,
		customer_engagement,
		net_revenue,
		cc_net_revenue,
		aru,
		equivalent_aru,
		implied_ink_yield,
		implied_toner_yield,
		implied_ci_ndp, 
		currency_impact
""")

ar_revenue_plugs_mix_calc.createOrReplaceTempView("ar_revenue_plugs_mix_calc")

# COMMAND ----------

spread_revenue_related_plugs_to_skus = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market10,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		sum(net_rev_plug * ifnull(rept_rev_mix, 1)) as net_revenue,
		ifnull(sum((net_rev_plug * ifnull(rept_rev_mix, 1))/nullif(aru, 0)), 0) as revenue_units,
		ifnull(sum((net_rev_plug * ifnull(rept_rev_mix, 1))/nullif(aru, 0)), 0) as equivalent_units,
		ifnull(sum(((net_rev_plug * ifnull(rept_rev_mix, 1))/nullif(aru, 0)) * implied_ink_yield), 0) as yield_x_units,
		ifnull(sum(((net_rev_plug * ifnull(rept_rev_mix, 1))/nullif(aru, 0)) * implied_toner_yield), 0) as yield_x_units_black_only,
		aru,
		equivalent_aru,
		implied_ink_yield,
		implied_toner_yield,
		sum(cc_rev_plug * ifnull(rept_cc_rev_mix, 1)) as cc_net_revenue,
		implied_ci_ndp
  FROM ar_revenue_plugs_mix_calc mix
  JOIN target_plugs_to_fill_gap plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market10 = plugs.market10 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market10,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		aru,
		equivalent_aru,
		implied_ink_yield,
		implied_toner_yield,
		implied_ci_ndp
""")

spread_revenue_related_plugs_to_skus.createOrReplaceTempView("spread_revenue_related_plugs_to_skus")

# COMMAND ----------

fully_baked_with_revenue_and_cc_rev_gaps_spread = spark.sql("""
with union_fully_baked_sku_data_with_revenue_gap AS
(
	SELECT 
     cal_date
      ,country_alpha2
      ,market10
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
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
    FROM fully_baked_sku_level_data
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

	  UNION ALL

	  SELECT 
       cal_date
      ,country_alpha2
      ,market10
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
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,0 as ci_usd
	  ,0 as ci_qty
	  ,0 as inventory_change_impact
	  ,0 as cc_inventory_impact
	  ,0 as adjusted_revenue
	  ,implied_ci_ndp
    FROM spread_revenue_related_plugs_to_skus
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

)

SELECT 
     cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,ifnull(sum(net_revenue), 0) as net_revenue
      ,ifnull(sum(revenue_units), 0) as revenue_units
      ,ifnull(sum(equivalent_units), 0) as equivalent_units
      ,ifnull(sum(yield_x_units), 0) as yield_x_units
      ,ifnull(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,ifnull(sum(cc_net_revenue), 0) as cc_net_revenue
	  ,ifnull(sum(ci_usd), 0) as ci_usd
	  ,ifnull(sum(ci_qty), 0) as ci_qty
	  ,ifnull(sum(inventory_change_impact), 0) as inventory_change_impact
	  ,ifnull(sum(cc_inventory_impact), 0) as cc_inventory_impact
	  ,ifnull(sum(adjusted_revenue), 0) as adjusted_revenue
	  ,ifnull(implied_ci_ndp, 0) as implied_ci_ndp
    FROM union_fully_baked_sku_data_with_revenue_gap
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

fully_baked_with_revenue_and_cc_rev_gaps_spread.createOrReplaceTempView("fully_baked_with_revenue_and_cc_rev_gaps_spread")

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
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	FROM fully_baked_with_revenue_and_cc_rev_gaps_spread
    WHERE 1=1
		AND process_detail = 'SYSTEM_ORIGINATED_DATA' -- use original data (before rev and cc rev spread process)
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

original_fully_baked.createOrReplaceTempView("original_fully_baked")

# COMMAND ----------

ci_original_data_positive = spark.sql("""
SELECT
		cal_date,
		market10,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM original_fully_baked
	WHERE 1=1
	AND cc_inventory_impact > 0
	GROUP BY cal_date, market10, pl, customer_engagement
""")

ci_original_data_positive.createOrReplaceTempView("ci_original_data_positive")

# COMMAND ----------

ci_original_data_negative = spark.sql("""
SELECT
		cal_date,
		market10,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM original_fully_baked
	WHERE 1=1
	AND cc_inventory_impact < 0
	GROUP BY cal_date, market10, pl, customer_engagement
""")

ci_original_data_negative.createOrReplaceTempView("ci_original_data_negative")

# COMMAND ----------

positive_ci_change = spark.sql("""
SELECT 
       cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,implied_ci_ndp
	  ,CASE
		WHEN sum(inventory_change_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
		ELSE inventory_change_impact / sum(inventory_change_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
	  END AS ci_chg_mix
	  ,CASE
		WHEN sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
		ELSE cc_inventory_impact / sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
	  END AS cc_ci_chg_mix
	FROM original_fully_baked
    WHERE 1=1
		AND cc_inventory_impact > 0
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,inventory_change_impact
	  ,cc_inventory_impact
	  ,implied_ci_ndp
""")

positive_ci_change.createOrReplaceTempView("positive_ci_change")

# COMMAND ----------

negative_ci_change = spark.sql("""
SELECT 
       cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,implied_ci_ndp
	  ,CASE
		WHEN sum(inventory_change_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
		ELSE inventory_change_impact / sum(inventory_change_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
	  END AS ci_chg_mix
	  ,CASE
		WHEN sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement) = 0 THEN NULL
		ELSE cc_inventory_impact / sum(cc_inventory_impact) OVER (PARTITION BY cal_date, market10, pl, customer_engagement)
	  END AS cc_ci_chg_mix
	FROM original_fully_baked
    WHERE 1=1
		AND cc_inventory_impact < 0
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,inventory_change_impact
	  ,cc_inventory_impact	  
	  ,implied_ci_ndp
""")

negative_ci_change.createOrReplaceTempView("negative_ci_change")

# COMMAND ----------

ci_positive_totals = spark.sql("""
SELECT
		cal_date,
		market10,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM target_mkt10_data
	WHERE 1=1
	AND cc_inventory_impact > 0
	GROUP BY cal_date, market10, pl, customer_engagement
""")

ci_positive_totals.createOrReplaceTempView("ci_positive_totals")

# COMMAND ----------

ci_negative_totals = spark.sql("""
SELECT
		cal_date,
		market10,
		pl,
		customer_engagement,
		SUM(inventory_change_impact) as inventory_change_impact,
		sum(cc_inventory_impact) as cc_inventory_impact
	FROM target_mkt10_data
	WHERE 1=1
	AND cc_inventory_impact < 0
	GROUP BY cal_date, market10, pl, customer_engagement
""")

ci_negative_totals.createOrReplaceTempView("ci_negative_totals")

# COMMAND ----------

ci_positive_gap = spark.sql("""
SELECT
		gold.cal_date,
		gold.market10,
		gold.pl,
		gold.customer_engagement,
		ifnull(sum(total.inventory_change_impact), 0) as inventory_change_target,
		ifnull(sum(total.cc_inventory_impact), 0) as cc_inventory_target,
		ifnull(sum(gold.inventory_change_impact), 0) as inventory_change_gold,
		ifnull(sum(gold.cc_inventory_impact), 0) as cc_inventory_gold,
		ifnull(sum(total.inventory_change_impact), 0) - ifnull(sum(gold.inventory_change_impact), 0) as inventory_change_plug,
		ifnull(sum(total.cc_inventory_impact), 0) - ifnull(sum(gold.cc_inventory_impact), 0) as cc_inventory_plug
	FROM ci_original_data_positive gold
	LEFT JOIN  ci_positive_totals total ON
		total.cal_date = gold.cal_date AND
		total.market10 = gold.market10 AND
		total.pl = gold.pl AND
		total.customer_engagement = gold.customer_engagement
	WHERE 1=1
	GROUP BY gold.cal_date,
		gold.market10,
		gold.pl,
		gold.customer_engagement
""")

ci_positive_gap.createOrReplaceTempView("ci_positive_gap")

# COMMAND ----------

ci_negative_gap = spark.sql("""
SELECT
		gold.cal_date,
		gold.market10,
		gold.pl,
		gold.customer_engagement,
		ifnull(sum(total.inventory_change_impact), 0) as inventory_change_target,
		ifnull(sum(total.cc_inventory_impact), 0) as cc_inventory_target,
		ifnull(sum(gold.inventory_change_impact), 0) as inventory_change_gold,
		ifnull(sum(gold.cc_inventory_impact), 0) as cc_inventory_gold,
		ifnull(sum(total.inventory_change_impact), 0) - ifnull(sum(gold.inventory_change_impact), 0) as inventory_change_plug,
		ifnull(sum(total.cc_inventory_impact), 0) - ifnull(sum(gold.cc_inventory_impact), 0) as cc_inventory_plug
	FROM ci_original_data_negative gold
	LEFT JOIN ci_negative_totals total ON
		total.cal_date = gold.cal_date AND
		total.market10 = gold.market10 AND
		total.pl = gold.pl AND
		total.customer_engagement = gold.customer_engagement
	WHERE 1=1
	GROUP BY gold.cal_date,
		gold.market10,
		gold.pl,
		gold.customer_engagement
""")

ci_negative_gap.createOrReplaceTempView("ci_negative_gap")

# COMMAND ----------

ci_positive_plug = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market10,
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
		0 as aru,
		0 as equivalent_aru,
		0 as implied_ink_yield,
		0 as implied_toner_yield,
		0 as cc_net_revenue,
		0 as ci_usd,
		0 as ci_qty,
		sum(inventory_change_plug * ifnull(ci_chg_mix, 0)) as inventory_change_impact,
		sum(cc_inventory_plug * ifnull(cc_ci_chg_mix, 0)) as cc_inventory_impact,
		0 as adjusted_revenue,
		implied_ci_ndp
  FROM ci_positive_gap mix 
  JOIN positive_ci_change plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market10 = plugs.market10 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market10,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		implied_ci_ndp
""")

ci_positive_plug.createOrReplaceTempView("ci_positive_plug")

# COMMAND ----------

ci_negative_plug = spark.sql("""
SELECT 
		mix.cal_date,
		country_alpha2,
		mix.market10,
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
		0 as aru,
		0 as equivalent_aru,
		0 as implied_ink_yield,
		0 as implied_toner_yield,
		0 as cc_net_revenue,
		0 as ci_usd,
		0 as ci_qty,
		sum(inventory_change_plug * ifnull(ci_chg_mix, 0)) as inventory_change_impact,
		sum(cc_inventory_plug * ifnull(cc_ci_chg_mix, 0)) as cc_inventory_impact,
		0 as adjusted_revenue,
		implied_ci_ndp
  FROM ci_negative_gap mix 
  JOIN negative_ci_change plugs ON
	mix.cal_date = plugs.cal_date AND
	mix.market10 = plugs.market10 AND
	mix.pl = plugs.pl AND
	mix.customer_engagement = plugs.customer_engagement
  WHERE 1=1
  GROUP BY	mix.cal_date,
		country_alpha2,
		mix.market10,
		region_5,
		platform_subset,
		base_product_number,
		mix.pl,
		mix.customer_engagement,
		implied_ci_ndp
""")

ci_negative_plug.createOrReplaceTempView("ci_negative_plug")

# COMMAND ----------

adjusted_revenue_mash1 = spark.sql("""
 with adjusted_rept_and_cc_rev_join_adjusted_ci_data AS
 (
	SELECT 
       cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	FROM fully_baked_with_revenue_and_cc_rev_gaps_spread
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

	  UNION ALL

	SELECT 
       cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	FROM ci_positive_plug
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

	UNION ALL
	  
	SELECT 
       cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	FROM ci_negative_plug
    WHERE 1=1	
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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
 )


 
SELECT 
     cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,ifnull(sum(net_revenue), 0) as net_revenue
      ,ifnull(sum(revenue_units), 0) as revenue_units
      ,ifnull(sum(equivalent_units), 0) as equivalent_units
      ,ifnull(sum(yield_x_units), 0) as yield_x_units
      ,ifnull(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,ifnull(sum(cc_net_revenue), 0) as cc_net_revenue
	  ,ifnull(sum(ci_usd), 0) as ci_usd
	  ,ifnull(sum(ci_qty), 0) as ci_qty
	  ,ifnull(sum(inventory_change_impact), 0) as inventory_change_impact
	  ,ifnull(sum(cc_inventory_impact), 0) as cc_inventory_impact
	  ,ifnull(sum(adjusted_revenue), 0) as adjusted_revenue
	  ,ifnull(implied_ci_ndp, 0) as implied_ci_ndp
    FROM adjusted_rept_and_cc_rev_join_adjusted_ci_data
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

adjusted_revenue_mash1.createOrReplaceTempView("adjusted_revenue_mash1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Thirteen
# MAGIC Spread MPS JV across OPS (Michael Doss requirement; Audrey doesn't want anymore)

# COMMAND ----------

adjusted_revenue_mash2 = spark.sql("""
SELECT 
       cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,ifnull(sum(net_revenue) / nullif(aru, 0), 0) as revenue_units
      ,ifnull(sum(net_revenue) / nullif(equivalent_aru, 0), 0) as equivalent_units
      ,ifnull(sum(net_revenue) / nullif(aru, 0), 0) * ifnull(sum(implied_ink_yield), 0) as yield_x_units
      ,ifnull(sum(net_revenue) / nullif(aru, 0), 0) * ifnull(sum(implied_toner_yield), 0) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(cc_net_revenue) - sum(cc_inventory_impact) as adjusted_revenue
	  ,implied_ci_ndp
	  ,ifnull(sum(inventory_change_impact) / nullif(implied_ci_ndp, 0), 0) as ci_unit_change
	  ,ifnull(sum(inventory_change_impact) / nullif(implied_ci_ndp, 0), 0) * ifnull(sum(implied_ink_yield), 0) as ci_yield_x_units
      ,ifnull(sum(inventory_change_impact) / nullif(implied_ci_ndp, 0), 0) * ifnull(sum(implied_toner_yield), 0) as ci_yield_x_units_black_only
    FROM adjusted_revenue_mash1
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

adjusted_revenue_mash2.createOrReplaceTempView("adjusted_revenue_mash2")

# COMMAND ----------

toner_ops_data = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	  ,sum(ci_unit_change) as ci_unit_change
	  ,sum(ci_yield_x_units) as ci_yield_x_units
      ,sum(ci_yield_x_units_black_only) as ci_yield_x_units_black_only
	FROM adjusted_revenue_mash2
    WHERE 1=1
		AND pl IN (select pl from mdm.product_line_xref 
					where pl_category = 'SUP' 
					and technology = 'LASER' 
					and business_division = 'OPS'
					and pl NOT IN ('GY', 'LZ')
					)
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

toner_ops_data.createOrReplaceTempView("toner_ops_data")

# COMMAND ----------

not_toner_ops_data = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	  ,sum(ci_unit_change) as ci_unit_change
	  ,sum(ci_yield_x_units) as ci_yield_x_units
      ,sum(ci_yield_x_units_black_only) as ci_yield_x_units_black_only
	FROM adjusted_revenue_mash2
    WHERE 1=1
		AND pl NOT IN (select distinct pl from toner_ops_data)
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

not_toner_ops_data.createOrReplaceTempView("not_toner_ops_data")

# COMMAND ----------

mps_jv = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,sum(net_revenue) as net_revenue
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(adjusted_revenue) as adjusted_revenue
	FROM toner_ops_data
    WHERE 1=1
		AND base_product_number = 'EST_MPS_REVENUE_JV'
    GROUP BY  cal_date
      ,country_alpha2
""")

mps_jv.createOrReplaceTempView("mps_jv")

# COMMAND ----------

toner_ops_without_mps = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	  ,sum(ci_unit_change) as ci_unit_change
	  ,sum(ci_yield_x_units) as ci_yield_x_units
      ,sum(ci_yield_x_units_black_only) as ci_yield_x_units_black_only
	FROM toner_ops_data
    WHERE 1=1
		AND base_product_number <> 'EST_MPS_REVENUE_JV'
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

toner_ops_without_mps.createOrReplaceTempView("toner_ops_without_mps")

# COMMAND ----------

toner_ops_mix = spark.sql("""
SELECT 
      cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,case
		when sum(net_revenue) over (partition by cal_date, country_alpha2) = 0 then null
		else net_revenue / sum(net_revenue) over (partition by cal_date, country_alpha2)
	  end as net_rev_mix
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,implied_ci_ndp
	FROM toner_ops_without_mps
    WHERE 1=1
		AND process_detail NOT LIKE '%ALLOCATION%'
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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
	  ,net_revenue
""")

toner_ops_mix.createOrReplaceTempView("toner_ops_mix")

# COMMAND ----------

spread_mps = spark.sql("""
SELECT 
      mix.cal_date
      ,mix.country_alpha2
      ,mix.market10
	  ,mix.region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,'MPS_ALLOCATION' as process_detail
      ,sum(net_revenue * ifnull(net_rev_mix, 1)) as net_revenue
      ,0 as revenue_units
      ,0 as equivalent_units
      ,0 as yield_x_units
      ,0 as yield_x_units_black_only
	  ,mix.aru 
	  ,mix.equivalent_aru
	  ,mix.implied_ink_yield
	  ,mix.implied_toner_yield
	  ,sum(cc_net_revenue * ifnull(net_rev_mix, 1)) as cc_net_revenue
	  ,0 as ci_usd
	  ,0 as ci_qty
	  ,0 as inventory_change_impact
	  ,0 as cc_inventory_impact
	  ,sum(adjusted_revenue * ifnull(net_rev_mix, 1)) as adjusted_revenue
	  ,mix.implied_ci_ndp
	  ,0 as ci_unit_change
	  ,0 as ci_yield_x_units
      ,0 as ci_yield_x_units_black_only
	FROM mps_jv jv
	JOIN toner_ops_mix mix 
        ON mix.cal_date = jv.cal_date 
        AND mix.country_alpha2 = jv.country_alpha2
    WHERE 1=1
    GROUP BY mix.cal_date
      ,mix.country_alpha2
      ,mix.market10
	  ,mix.region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,mix.aru 
	  ,mix.equivalent_aru
	  ,mix.implied_ink_yield
	  ,mix.implied_toner_yield
	  ,mix.implied_ci_ndp
""")

spread_mps.createOrReplaceTempView("spread_mps")

# COMMAND ----------

adjusted_revenue_mash3 = spark.sql("""
WITH rejoin_mps_spread_with_ops_and_non_ops AS
(
	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	  ,sum(ci_unit_change) as ci_unit_change
	  ,sum(ci_yield_x_units) as ci_yield_x_units
      ,sum(ci_yield_x_units_black_only) as ci_yield_x_units_black_only
	FROM  not_toner_ops_data
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

	UNION ALL

	SELECT 
      cal_date
      ,country_alpha2
      ,market10
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
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(adjusted_revenue) as adjusted_revenue
	  ,implied_ci_ndp
	  ,sum(ci_unit_change) as ci_unit_change
	  ,sum(ci_yield_x_units) as ci_yield_x_units
      ,sum(ci_yield_x_units_black_only) as ci_yield_x_units_black_only
	FROM toner_ops_without_mps
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

	UNION ALL

	SELECT 
       cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,sum(net_revenue) as net_revenue
      ,ifnull(sum(net_revenue) / nullif(aru, 0), 0) as revenue_units
      ,ifnull(sum(net_revenue) / nullif(equivalent_aru, 0), 0) as equivalent_units
      ,ifnull(sum(net_revenue) / nullif(aru, 0), 0) * ifnull(sum(implied_ink_yield), 0) as yield_x_units
      ,ifnull(sum(net_revenue) / nullif(aru, 0), 0) * ifnull(sum(implied_toner_yield), 0) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,sum(cc_net_revenue) as cc_net_revenue
	  ,sum(ci_usd) as ci_usd
	  ,sum(ci_qty) as ci_qty
	  ,sum(inventory_change_impact) as inventory_change_impact
	  ,sum(cc_inventory_impact) as cc_inventory_impact
	  ,sum(cc_net_revenue) - sum(cc_inventory_impact) as adjusted_revenue
	  ,implied_ci_ndp
	  ,ifnull(sum(inventory_change_impact) / nullif(implied_ci_ndp, 0), 0) as ci_unit_change
	  ,ifnull(sum(inventory_change_impact) / nullif(implied_ci_ndp, 0), 0) * ifnull(sum(implied_ink_yield), 0) as ci_yield_x_units
      ,ifnull(sum(inventory_change_impact) / nullif(implied_ci_ndp, 0), 0) * ifnull(sum(implied_toner_yield), 0) as ci_yield_x_units_black_only
    FROM spread_mps
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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
)

 
SELECT 
     cal_date
      ,country_alpha2
      ,market10
	  ,region_5
      ,platform_subset
      ,base_product_number
      ,pl
      ,customer_engagement
	  ,process_detail
      ,ifnull(sum(net_revenue), 0) as net_revenue
      ,ifnull(sum(revenue_units), 0) as revenue_units
      ,ifnull(sum(equivalent_units), 0) as equivalent_units
      ,ifnull(sum(yield_x_units), 0) as yield_x_units
      ,ifnull(sum(yield_x_units_black_only), 0) as yield_x_units_black_only
	  ,aru 
	  ,equivalent_aru
	  ,implied_ink_yield
	  ,implied_toner_yield
	  ,ifnull(sum(cc_net_revenue), 0) as cc_net_revenue
	  ,ifnull(sum(ci_usd), 0) as ci_usd
	  ,ifnull(sum(ci_qty), 0) as ci_qty
	  ,ifnull(sum(inventory_change_impact), 0) as inventory_change_impact
	  ,ifnull(sum(cc_inventory_impact), 0) as cc_inventory_impact
	  ,ifnull(sum(adjusted_revenue), 0) as adjusted_revenue
	  ,implied_ci_ndp as implied_ci_ndp
	  ,ifnull(sum(ci_unit_change), 0) as ci_unit_change
	  ,ifnull(sum(ci_yield_x_units) ,0) as ci_yield_x_units
	  ,ifnull(sum(ci_yield_x_units_black_only), 0) as ci_yield_x_units_black_only
	FROM rejoin_mps_spread_with_ops_and_non_ops
    WHERE 1=1
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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

adjusted_revenue_mash3.createOrReplaceTempView("adjusted_revenue_mash3")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adjusted Revenue EPA Staging

# COMMAND ----------

adj_rev_staging = spark.sql("""
SELECT 'ACTUALS - ADJUSTED_REVENUE_EPA' as record
      ,cal_date
      ,country_alpha2
      ,market10
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
	FROM adjusted_revenue_mash3
    WHERE 1=1	
    GROUP BY  cal_date
      ,country_alpha2
      ,market10
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
      ,market10 as GEOGRAPHY
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
      ,market10
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
      ,market10
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
      ,market10
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
      ,market10
	  ,region_5
      ,base_product_number
	  ,pl
      ,customer_engagement
      ,sum(net_revenue) as net_revenue
	  ,sum(revenue_units) as revenue_units
      ,sum(equivalent_units) as equivalent_units
      ,sum(yield_x_units) as yield_x_units
      ,sum(yield_x_units_black_only) as yield_x_units_black_only
	FROM baseprod_targets_detailed
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
      ,market10
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
