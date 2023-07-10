# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# submit query to redshift

query = f"""

    CALL prod.addversion_sproc('ACTUALS - ACTUAL SUPPLIES', 'ACTUALS - ACTUAL SUPPLIES');
    
    TRUNCATE prod.actuals_supplies;

    INSERT INTO prod.actuals_supplies
            (record,
            cal_date,
            country_alpha2,
            market10,
            platform_subset,
            base_product_number,
            customer_engagement,
            base_quantity,
            equivalent_units,
            yield_x_units,
            yield_x_units_black_only,
            official,
            load_date,
            version)
	SELECT 
		'ACTUALS - SUPPLIES' AS record,
		cal_date,
		country_alpha2,
		market10,
		platform_subset,
		base_product_number,
		customer_engagement,
		CAST(SUM(revenue_units) AS decimal(18, 4)) AS base_quantity,
		SUM(equivalent_units) AS equivalent_units,
		SUM(yield_x_units) AS yield_x_units,
		SUM(yield_x_units_black_only) AS yield_x_units_black_only,
		1 AS official, -- at a later time, tie to the version table
		(SELECT load_date from prod.version WHERE record = 'ACTUALS - ACTUAL SUPPLIES'
			AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'ACTUALS - ACTUAL SUPPLIES')) AS load_date,
		(SELECT version from prod.version WHERE record = 'ACTUALS - ACTUAL SUPPLIES' 
			AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - ACTUAL SUPPLIES')) AS version
	FROM fin_prod.actuals_supplies_baseprod
	WHERE 1=1
	AND revenue_units != 0
    AND country_alpha2 NOT IN ('BY', 'RU', 'CU', 'IR', 'KP', 'SY')
	GROUP BY record, cal_date, country_alpha2, base_product_number, version, market10, customer_engagement, platform_subset
"""

submit_remote_query(configs, query)
