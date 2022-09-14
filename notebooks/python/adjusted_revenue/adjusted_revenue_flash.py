# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue Flash
# MAGIC 
# MAGIC Tables Needed in Delta Lake:
# MAGIC - fin_prod.adjusted_revenue_salesprod
# MAGIC - fin_prod.supplies_finance_flash

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add Version

# COMMAND ----------

add_record = 'ADJ REV PLUS FLASH'
add_source = 'ADJREV PLUS SUPPLIES FLASH'

# COMMAND ----------

adj_flsh_add_version = call_redshift_addversion_sproc(configs, add_record, add_source)

version = adj_flsh_add_version[0]
print(version)

# COMMAND ----------

adj_rev_data = spark.sql("""
with
    adjusted_revenue_staging as 
    (
        select 
            fiscal_year_qtr,
            fiscal_yr,
            ar.market10,
            'NON-HQ' as hq_flag,
            pl,
            accounting_rate,
            sum(net_revenue) as reported_revenue,
            sum(net_hedge_benefit) as hedge,
            sum(currency_impact) as currency,
            sum(cc_net_revenue) as revenue_in_cc,
            sum(inventory_change_impact) as inventory_change,
            sum(currency_impact_ch_inventory) as ci_currency_impact,
            sum(cc_inventory_impact) as total_inventory_impact,
            sum(adjusted_revenue) as adjusted_revenue
        from fin_prod.adjusted_revenue_salesprod ar
        join mdm.calendar cal on cal.date = ar.cal_date -- add fiscal quarter
        join mdm.iso_country_code_xref iso on iso.country_alpha2 = ar.country_alpha2 
        where day_of_month = 1
            and ar.version = (select max(version) from fin_prod.adjusted_revenue_salesprod) -- e.g. '2021.06.08.2'
            -- not in any future quarter provided..
            and fiscal_year_qtr not in (
                select distinct fiscal_year_qtr 
                from fin_prod.supplies_finance_flash 
                where net_revenue <> 0 
                and version = (select max(version) from fin_prod.supplies_finance_flash)
            )
            and pl in (
                select distinct pl 
                from mdm.product_line_xref 
                where pl_category = 'SUP' 
                    and technology in ('INK', 'PWA', 'LASER') 
                    and pl not in ('GY', 'LZ')
            )
        group by 
            fiscal_year_qtr,
            fiscal_yr,
            ar.market10,
            pl,
            accounting_rate
    )

        select
            fiscal_year_qtr,
            fiscal_yr,
            market10,
            hq_flag,
            pl,
            accounting_rate,
            sum(reported_revenue) as reported_revenue,
            sum(hedge) as hedge,
            sum(currency) as currency,
            sum(revenue_in_cc) as revenue_in_cc,
            sum(inventory_change) as inventory_change,
            sum(ci_currency_impact) as ci_currency_impact,
            sum(total_inventory_impact) as total_inventory_impact,
            sum(adjusted_revenue) as adjusted_revenue
        from adjusted_revenue_staging
        group by fiscal_year_qtr,
            fiscal_yr,
            market10,
            hq_flag,
            pl,
            accounting_rate
""")

adj_rev_data.createOrReplaceTempView("adjusted_revenue_data")

# COMMAND ----------


