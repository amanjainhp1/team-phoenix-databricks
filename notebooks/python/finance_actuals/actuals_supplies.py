# Databricks notebook source
dbutils.widgets.text("load_to_redshift", "")

# COMMAND ----------

# MAGIC %run Repos/noelle.diederich@hp.com/team-phoenix-databricks/notebooks/python/common/configs

# COMMAND ----------

# MAGIC %run Repos/noelle.diederich@hp.com/team-phoenix-databricks/notebooks/python/common/database_utils

# COMMAND ----------

if dbutils.widgets.get("load_to_redshift").lower() == "true": 
    
    #actuals_supplies = read_sql_server_to_df(configs) \
    #    .option("dbtable", "IE2_Prod.dbo.actuals_supplies") \
    #    .load()

    #write_df_to_redshift(configs, actuals_supplies, "prod.actuals_supplies", "append", "", "truncate prod.actuals_supplies")
    
    actuals_supplies_salesprod = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Financials.dbo.actuals_supplies_salesprod") \
        .load()

    write_df_to_redshift(configs, actuals_supplies_salesprod, "fin_prod.actuals_supplies_salesprod", "append", "", "truncate fin_prod.actuals_supplies_salesprod")

    actuals_supplies_baseprod = read_sql_server_to_df(configs) \
        .option("dbtable", "IE2_Financials.dbo.actuals_supplies_baseprod") \
        .load()

    write_df_to_redshift(configs, actuals_supplies_baseprod, "fin_prod.actuals_supplies_baseprod", "append", "", "truncate fin_prod.actuals_supplies_baseprod")

# COMMAND ----------

# method 1: load redshift data to dataframes, transform dataframes, load to redshift

actuals_supplies_salesprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_salesprod") \
    .load()

actuals_supplies_baseprod = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.actuals_supplies_baseprod") \
    .load()

actuals_supplies_salesprod.createOrReplaceTempView("actuals_supplies_salesprod")
actuals_supplies_baseprod.createOrReplaceTempView("actuals_supplies_baseprod")

addversion_info = call_redshift_addversion_sproc(configs, "ACTUALS - ODW SUPPLIES", "ODW")

preaction_query = """
    DELETE FROM prod.actuals_supplies
    WHERE cal_date = (select max(cal_date) from fin_prod.actuals_supplies_salesprod)
"""

query = f"""
	SELECT 
		'ACTUALS - SUPPLIES' AS record,
		cal_date,
		country_alpha2,
		market10,
		platform_subset,
		base_product_number,
		customer_engagement,
		SUM(revenue_units) AS base_quantity,
		SUM(equivalent_units) AS equivalent_units,
		SUM(yield_x_units) AS yield_x_units,
		SUM(yield_x_units_black_only) AS yield_x_units_black_only,
		1 AS official, -- at a later time, tie to the version table
		'{addversion_info[1]}' AS load_date,
		'{addversion_info[0]}' AS version
	FROM actuals_supplies_baseprod
	WHERE 1=1
	AND revenue_units != 0
	AND cal_date = (select max(cal_date) from actuals_supplies_baseprod)
	GROUP BY record, cal_date, country_alpha2, base_product_number, version, market10, customer_engagement, platform_subset
"""

actuals_supplies = spark.sql(query)

write_df_to_redshift(configs, actuals_supplies, "prod.actuals_supplies", "append", postactions = "", preactions = preaction_query)

# COMMAND ----------

# method 2: submit query to redshift

query = f"""

    CALL prod.addversion_sproc('ACTUALS - ODW SUPPLIES', 'ODW');
    
    DELETE FROM prod.actuals_supplies
    WHERE cal_date = (select max(cal_date) from fin_prod.actuals_supplies_salesprod);

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
		SUM(revenue_units) AS base_quantity,
		SUM(equivalent_units) AS equivalent_units,
		SUM(yield_x_units) AS yield_x_units,
		SUM(yield_x_units_black_only) AS yield_x_units_black_only,
		1 AS official, -- at a later time, tie to the version table
		(SELECT load_date from prod.version WHERE record = 'ACTUALS - ODW SUPPLIES'
			AND load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'ACTUALS - ODW SUPPLIES')) AS load_date,
		(SELECT version from prod.version WHERE record = 'ACTUALS - ODW SUPPLIES' 
			AND version = (SELECT MAX(version) FROM prod.version WHERE record = 'ACTUALS - ODW SUPPLIES')) AS version
	FROM fin_prod.actuals_supplies_baseprod
	WHERE 1=1
	AND revenue_units != 0
	AND cal_date = (select max(cal_date) from fin_prod.actuals_supplies_baseprod)
	GROUP BY record, cal_date, country_alpha2, base_product_number, version, market10, customer_engagement, platform_subset
"""

submit_remote_query(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], query)

# COMMAND ----------


