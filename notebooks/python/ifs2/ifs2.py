# Databricks notebook source
# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

ifs2_qtr = f"""

with ifs2_filter_dates as (


SELECT
    cast('FORECAST_SUPPLIES_BASEPROD' as text) as record
    , MIN(cal_date) as cal_date
FROM "fin_prod"."forecast_supplies_baseprod"    
WHERE
    version = '{dbutils.widgets.get("forecast_fin_version")}'
),ifs2_inputs as (


SELECT cast('FORECAST_SUPPLIES_BASEPROD' AS text) AS record
    , MAX(version) AS version
FROM "fin_prod"."forecast_supplies_baseprod"

UNION ALL

SELECT 'NORM_SHIPMENTS' AS record
    , MAX(version) AS version
FROM "prod"."norm_shipments"

UNION ALL

SELECT 'IB' AS record
    , MAX(version) AS version
FROM "prod"."ib"
),ifs2_03_baseprod_financials AS (


SELECT DISTINCT
	demand.platform_subset
	, demand.customer_engagement
	--, baseprod.base_product_number
	, baseprod.region_5
	, baseprod.country_alpha2
	, baseprod.cal_date
	, calendar.Fiscal_Yr
	, calendar.Fiscal_Month
	, calendar.Fiscal_Year_Qtr 
	, SUM(coalesce(demand.cartridges, 0)) cartridges
	, SUM(coalesce(demand.imp_corrected_cartridges, 0)) imp_corrected_cartridges
	, SUM(coalesce(demand.imp_corrected_cartridges, 0)*COALESCE(baseprod.BaseProd_GRU, 0)) GrossRev
	, SUM(coalesce(demand.imp_corrected_cartridges, 0)*COALESCE(baseprod.BaseProd_Contra_perUnit, 0)) Contra
	, SUM(coalesce(demand.imp_corrected_cartridges, 0)*COALESCE(baseprod.BaseProd_RevenueCurrencyHedge_Unit, 0)) RevenueCurrencyHedge
	, SUM(coalesce(demand.imp_corrected_cartridges, 0)*COALESCE(baseprod.BaseProd_VariableCost_perUnit, 0)) VariableCost
	, SUM(coalesce(demand.imp_corrected_cartridges, 0)*COALESCE(baseprod.BaseProd_FixedCost_perUnit, 0)) FixedCost
    , baseprod.version AS fin_version
    , demand.version AS demand_version
FROM
    (
    SELECT version
          , base_product_number
          , region_5
          , country_alpha2
          , cal_date
          , BaseProd_GRU
          , BaseProd_Contra_perUnit
          , BaseProd_RevenueCurrencyHedge_Unit
          , BaseProd_VariableCost_perUnit
          , BaseProd_FixedCost_perUnit 
    FROM  "fin_prod"."forecast_supplies_baseprod"
    WHERE version = '{dbutils.widgets.get('forecast_fin_version')}' and BaseProd_GRU IS NOT NULL
    ) AS baseprod
INNER JOIN
    (
     SELECT platform_subset
          , customer_engagement
          , base_product_number
          , cal_date, country
          , imp_corrected_cartridges
          , cartridges
          , version 
   FROM "prod"."working_forecast_country" 
   WHERE version = '{dbutils.widgets.get('fin_ifs2_cart_version')}'
     AND cal_date >= (SELECT MIN(cal_date) FROM "fin_prod"."forecast_supplies_baseprod" WHERE version = '{dbutils.widgets.get('forecast_fin_version')}')
     AND cal_date <= '{dbutils.widgets.get('max_ifs2_date')}'
    ) AS demand
ON
    demand.base_product_number = baseprod.base_product_number
    AND demand.cal_date = baseprod.cal_date
    AND demand.country = baseprod.country_alpha2
INNER JOIN "mdm"."calendar" calendar
    ON calendar.date = baseprod.cal_date
WHERE
    baseprod.BaseProd_GRU IS NOT NULL
    AND baseprod.BaseProd_GRU >= 1
GROUP BY
    demand.platform_subset
    , demand.customer_engagement
    , baseprod.region_5
    , baseprod.country_alpha2
    , baseprod.cal_date
    , calendar.Fiscal_Yr
    , calendar.Fiscal_Month
    , calendar.Fiscal_Year_Qtr
    , baseprod.version
    , demand.version    
),ib_02a_ce_splits as (


SELECT ce.record
    , ce.platform_subset
    , ce.region_5
    , ce.country_alpha2
    , ce.month_begin
    , ce.split_name
    , ce.pre_post_flag
    , ce.value
    , ce.load_date
FROM "prod"."ce_splits" AS ce
WHERE 1=1
    AND ce.official = 1
    AND ce.record IN ('CE_SPLITS_I-INK', 'CE_SPLITS_I-INK LF')
),  ib_02b_ce_splits_filter as (


SELECT DISTINCT record
    , platform_subset
    , country_alpha2
    , split_name
    , load_date
    , ROW_NUMBER() OVER (PARTITION BY platform_subset, country_alpha2, split_name ORDER BY load_date DESC) AS load_select
FROM
(
    SELECT DISTINCT ce.record
        , ce.platform_subset
        , ce.country_alpha2
        , ce.split_name
        , ce.load_date
    FROM ib_02a_ce_splits AS ce
    WHERE 1=1
        AND pre_post_flag = 'PRE'
) AS sub
WHERE 1=1
),  ib_02c_ce_splits_final AS (


SELECT ce.record
    , ce.platform_subset
    , ce.region_5
    , ce.country_alpha2
    , ce.month_begin
    , ce.split_name
    , ce.pre_post_flag
    , ce.value
    , ce.load_date
FROM ib_02a_ce_splits AS ce
JOIN ib_02b_ce_splits_filter AS f
    ON f.record = ce.record
    AND f.load_date = ce.load_date
    AND f.platform_subset = ce.platform_subset
    AND f.country_alpha2 = ce.country_alpha2
    AND f.split_name = ce.split_name
WHERE 1=1
    AND ce.pre_post_flag = 'PRE'
    AND f.load_select = 1
), ifs2_04_ce_splits AS (


SELECT ib.cal_date AS month_begin
	, ib.platform_subset
	, ib.country_alpha2 
	, CASE WHEN ib.platform_subset LIKE '%STND%' THEN 'STD'
           WHEN ib.platform_subset LIKE '%YET2%' THEN 'HP+'
           ELSE 'TRAD' END AS split_name
	, 1.0 AS value
FROM "prod"."ib" ib
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
WHERE 1=1
    AND hw.technology IN ('LASER')
    AND ib.version = '{dbutils.widgets.get('fin_ib_version')}'

UNION ALL

SELECT r.month_begin
    , r.platform_subset
    , r.country_alpha2
    , r.split_name
    , r.value
FROM ib_02c_ce_splits_final AS r
JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = r.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'PWA')
), ifs2_05_hw_forecast as (


SELECT
    ns.cal_date
    , ns.country_alpha2
    , ns.platform_subset
    , COALESCE(ce.split_name, 'TRAD') AS customer_engagement
    , (COALESCE(ce.value, 1) * units) HW_Units
    , ROW_NUMBER() OVER (PARTITION BY ns.platform_subset, ns.country_alpha2 ORDER BY ns.cal_date) AS HW_rownum
    , ns.version
FROM
     "prod"."norm_shipments" ns
LEFT JOIN
    "prod"."ce_splits" ce
        ON ce.month_begin = ns.cal_date
        AND ce.country_alpha2 = ns.country_alpha2
        AND ce.platform_subset = ns.platform_subset
WHERE
    ns.version = '{dbutils.widgets.get("fin_ns_version")}'
    AND units >= 1
    --and platform_subset = 'SELENE YET1 DM2' and country_alpha2 = 'GB'
    AND cal_date >= (SELECT cal_date FROM ifs2_filter_dates WHERE record = 'FORECAST_SUPPLIES_BASEPROD')    
), ifs2_06_ib AS (


SELECT
    [cal_date]
    , country_alpha2 
    , [platform_subset]
    , [customer_engagement]
    , units
FROM
    "prod"."ib"
WHERE
    measure = 'IB'
    AND units >= 1
    AND version = '{dbutils.widgets.get("fin_ib_version")}'
    --and platform_subset = 'SELENE YET1 DM2'
    AND [cal_date] >= (SELECT cal_date FROM ifs2_filter_dates WHERE record = 'FORECAST_SUPPLIES_BASEPROD')    
), ifs2_07_financials_ib as (


SELECT
    Financials_1.platform_subset
    , Financials_1.customer_engagement
    , Financials_1.region_5
    , Financials_1.country_alpha2
    , Financials_1.cal_date
    , Financials_1.Fiscal_Yr
    , Financials_1.Fiscal_Month
    , Financials_1.Fiscal_Year_Qtr
    --, Financials_1.Net_Revenue
    --, Financials_1.ContributionMargin
    --, Financials_1.GrossMargin
    , ((Financials_1.GrossRev - Financials_1.Contra+Financials_1.RevenueCurrencyHedge)/nullif(ib.units, 0)) NetRevenue_IB
    , ((Financials_1.GrossRev - Financials_1.Contra+Financials_1.RevenueCurrencyHedge - Financials_1.VariableCost)/nullif(ib.units, 0)) ContributionMargin_IB
    , ((Financials_1.GrossRev - Financials_1.Contra+Financials_1.RevenueCurrencyHedge - Financials_1.VariableCost - Financials_1.FixedCost)/nullif(ib.units, 0)) GrossMargin_IB
    , COALESCE(hw_forecast.HW_Units, 0) hw_units
    --, ib.units
    , COUNT(Financials_1.cal_date) OVER (PARTITION BY Financials_1.platform_subset, Financials_1.customer_engagement, Financials_1.region_5, Financials_1.country_alpha2) AS count_rows
    , ROW_NUMBER() OVER (PARTITION BY Financials_1.platform_subset, Financials_1.customer_engagement, Financials_1.region_5, Financials_1.country_alpha2 order by Financials_1.cal_date) AS Fin_IB_rownum
FROM
    ifs2_03_baseprod_financials Financials_1
    LEFT JOIN
    ifs2_05_hw_forecast	hw_forecast
        ON Financials_1.platform_subset = hw_forecast.platform_subset
        AND Financials_1.cal_date = hw_forecast.cal_date
        AND Financials_1.country_alpha2 = hw_forecast.country_alpha2
        AND Financials_1.customer_engagement = hw_forecast.customer_engagement
    LEFT JOIN
    ifs2_06_ib ib
        ON ib.platform_subset = Financials_1.platform_subset
        AND ib.cal_date = Financials_1.cal_date
        AND ib.country_alpha2 = Financials_1.country_alpha2
        AND ib.customer_engagement = Financials_1.customer_engagement        
), ifs2_08_calc_discount as (


SELECT
    hw.platform_subset
    , hw.country_alpha2
    , Fin_IB.customer_engagement
    , hw.cal_date as hw_cal_date
    , Fin_IB.cal_date as FinIB_cal_date
    , hw.HW_Units
    , Fin_IB.NetRevenue_IB
    , Fin_IB.ContributionMargin_IB
    , Fin_IB.GrossMargin_IB
    , pow((1+(0.09/12)),Fin_IB.Fin_IB_rownum) as Discounting_Factor
    , ROW_NUMBER() OVER (PARTITION BY hw.platform_subset, hw.country_alpha2, Fin_IB.customer_engagement, hw.cal_date ORDER BY Fin_IB.cal_date) AS row_num
FROM
    (
      SELECT 
        platform_subset
        , country_alpha2
        , cal_date
        , HW_Units
        , customer_engagement 
     FROM ifs2_05_hw_forecast 
     WHERE HW_rownum > 1
     ) hw
INNER JOIN
    ifs2_07_financials_ib Fin_IB
    ON hw.platform_subset = Fin_IB.platform_subset
    AND hw.country_alpha2 = Fin_IB.country_alpha2
    AND hw.cal_date <= Fin_IB.cal_date
    AND hw.customer_engagement = Fin_IB.customer_engagement
), ib_07_years as (


SELECT 1 AS year_num UNION ALL
SELECT 2 AS year_num UNION ALL
SELECT 3 AS year_num UNION ALL
SELECT 4 AS year_num UNION ALL
SELECT 5 AS year_num UNION ALL
SELECT 6 AS year_num UNION ALL
SELECT 7 AS year_num UNION ALL
SELECT 8 AS year_num UNION ALL
SELECT 9 AS year_num UNION ALL
SELECT 10 AS year_num UNION ALL
SELECT 11 AS year_num UNION ALL
SELECT 12 AS year_num UNION ALL
SELECT 13 AS year_num UNION ALL
SELECT 14 AS year_num UNION ALL
SELECT 15 AS year_num UNION ALL
SELECT 16 AS year_num UNION ALL
SELECT 17 AS year_num UNION ALL
SELECT 18 AS year_num UNION ALL
SELECT 19 AS year_num UNION ALL
SELECT 20 AS year_num UNION ALL
SELECT 21 AS year_num UNION ALL
SELECT 22 AS year_num UNION ALL
SELECT 23 AS year_num UNION ALL
SELECT 24 AS year_num UNION ALL
SELECT 25 AS year_num UNION ALL
SELECT 26 AS year_num UNION ALL
SELECT 27 AS year_num UNION ALL
SELECT 28 AS year_num UNION ALL
SELECT 29 AS year_num UNION ALL
SELECT 30 AS year_num
),  ib_06_months as (


SELECT 1 AS month_num UNION ALL
SELECT 2 AS month_num UNION ALL
SELECT 3 AS month_num UNION ALL
SELECT 4 AS month_num UNION ALL
SELECT 5 AS month_num UNION ALL
SELECT 6 AS month_num UNION ALL
SELECT 7 AS month_num UNION ALL
SELECT 8 AS month_num UNION ALL
SELECT 9 AS month_num UNION ALL
SELECT 10 AS month_num UNION ALL
SELECT 11 AS month_num UNION ALL
SELECT 12 AS month_num
),  ib_08_decay_months as (


SELECT d.geography_grain
    , d.geography
    , d.platform_subset
    , d.split_name
    , (y.year_num - 1) * 12 + m.month_num - 1 AS month_offset
    , d.value / 12 AS decayed_amt
FROM "prod"."decay" AS d
JOIN ib_07_years AS y
    ON 'YEAR_' + CAST(y.year_num AS VARCHAR) = d.year
JOIN ib_06_months AS m
    ON 1=1
WHERE 1=1
    AND d.official = 1
    AND d.record IN ('HW_DECAY','HW_DECAY_LF')
),  ib_09_remaining_amt as (


SELECT geography_grain
    , geography
    , platform_subset
    , split_name
    , month_offset
    , CASE WHEN 1 - SUM(decayed_amt) OVER (PARTITION BY platform_subset, geography, split_name ORDER BY month_offset ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) > 0
           THEN 1 - SUM(decayed_amt) OVER (PARTITION BY platform_subset, geography, split_name ORDER BY month_offset ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           ELSE 0 END AS remaining_amt
FROM ib_08_decay_months AS decay
WHERE NOT decayed_amt IS NULL  -- remove any nulls
), ifs2_09_financials_decay_discount as (


SELECT
    printer_offset.platform_subset
    , printer_offset.country_alpha2
    , printer_offset.customer_engagement
    , printer_offset.hw_cal_date
    , printer_offset.FinIB_cal_date
    , printer_offset.row_num
    , printer_offset.NetRevenue_IB
    , printer_offset.HW_Units
    , printers_remaining_1.remaining_amt
    , (printers_remaining_1.remaining_amt * printer_offset.NetRevenue_IB) decay_rev
    , (printers_remaining_1.remaining_amt * printer_offset.NetRevenue_IB) decay_CM
    , (printers_remaining_1.remaining_amt * printer_offset.NetRevenue_IB) decay_GM
    , printer_offset.Discounting_Factor
    , (printers_remaining_1.remaining_amt * printer_offset.NetRevenue_IB)/printer_offset.Discounting_Factor as Discounted_Rev
    , (printers_remaining_1.remaining_amt * printer_offset.NetRevenue_IB * printer_offset.HW_Units)/printer_offset.Discounting_Factor as NPV_Rev
    , (printers_remaining_1.remaining_amt * printer_offset.ContributionMargin_IB)/printer_offset.Discounting_Factor as Discounted_CM
    , (printers_remaining_1.remaining_amt * printer_offset.ContributionMargin_IB * printer_offset.HW_Units)/printer_offset.Discounting_Factor as NPV_CM
    , (printers_remaining_1.remaining_amt * printer_offset.GrossMargin_IB)/printer_offset.Discounting_Factor as Discounted_GM
    , (printers_remaining_1.remaining_amt * printer_offset.GrossMargin_IB * printer_offset.HW_Units)/printer_offset.Discounting_Factor as NPV_GM
FROM
    ifs2_08_calc_discount  printer_offset
    INNER JOIN
    "mdm"."iso_country_code_xref" iso_country_code_xref
        ON printer_offset.country_alpha2 = iso_country_code_xref.country_alpha2
    INNER JOIN
    ib_09_remaining_amt printers_remaining_1
        ON printer_offset.platform_subset = printers_remaining_1.platform_subset
        AND iso_country_code_xref.region_5 = printers_remaining_1.geography
        AND printer_offset.row_num = printers_remaining_1.month_offset+1
), ifs2_10_financials_NPV as (


SELECT
    printer_decay.platform_subset
    , printer_decay.customer_engagement
    , printer_decay.country_alpha2
    , calendar.Fiscal_Year_Qtr
    , calendar.Fiscal_Yr
    , printer_decay.hw_cal_date
    , sum(printer_decay.NPV_Rev) as NPV_Rev
    , sum(printer_decay.NPV_CM) as NPV_CM
    , sum(printer_decay.NPV_GM) as NPV_GM
    , avg(hw_forecast.HW_Units) as HW_Units
FROM
    ifs2_09_financials_decay_discount printer_decay
    INNER JOIN
    ifs2_05_hw_forecast hw_forecast
        ON printer_decay.platform_subset = hw_forecast.platform_subset
        AND printer_decay.country_alpha2 = hw_forecast.country_alpha2
        AND printer_decay.hw_cal_date = hw_forecast.cal_date
        AND UPPER(printer_decay.customer_engagement) = UPPER(hw_forecast.customer_engagement)
    INNER JOIN
    "mdm"."calendar" calendar
        ON calendar.date = printer_decay.hw_cal_date
GROUP BY
    printer_decay.platform_subset
    , printer_decay.customer_engagement
    , printer_decay.country_alpha2
    , printer_decay.hw_cal_date
    , calendar.Fiscal_Year_Qtr
    , calendar.Fiscal_Yr
)
SELECT
    printer_npv.platform_subset
    , printer_npv.customer_engagement
    , printer_npv.country_alpha2
    , printer_npv.Fiscal_Year_Qtr
    , ISNULL(sum(printer_npv.NPV_Rev)/NULLIF(sum(printer_npv.HW_Units), 0), 0) as IFS2_NetRevenue
    , ISNULL(sum(printer_NPV.NPV_CM)/NULLIF(sum(printer_npv.HW_Units), 0), 0) as IFS2_CM
    , ISNULL(sum(printer_NPV.NPV_GM)/NULLIF(sum(printer_npv.HW_Units), 0), 0) as IFS2_GM
    , SUM(printer_NPV.HW_Units) as HW_Units
    , '{dbutils.widgets.get("forecast_fin_version")}' as fin_version
    , '{dbutils.widgets.get("fin_ib_version")}' as ib_version
    , '{dbutils.widgets.get("fin_ns_version")}' as ns_version
    , '{dbutils.widgets.get("fin_ifs2_plan")}' as ifs2_plan
FROM
    ifs2_10_financials_NPV printer_npv
GROUP BY
    printer_npv.platform_subset
    , printer_npv.customer_engagement
    , printer_npv.country_alpha2
    , printer_npv.Fiscal_Year_Qtr
"""
query_list.append(["stage.ifs2_qtr", ifs2_qtr, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list

# COMMAND ----------

# MAGIC %md
# MAGIC ## IFS2_Promotion

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

ifs2_promotion = f"""

with ifs2_filter_dates as (


SELECT
    CAST('FORECAST_SUPPLIES_BASEPROD' AS text) as record
    , MIN(cal_date) as cal_date
FROM "fin_prod"."forecast_supplies_baseprod"    
WHERE
    version = '{dbutils.widgets.get("forecast_fin_version")}'
), cartridge_financials as (


SELECT DISTINCT 
    demand.platform_subset
    , demand.customer_engagement
    , baseprod.base_product_number
    , baseprod.region_5
    , baseprod.country_alpha2
    , baseprod.cal_date
    , calendar.Fiscal_Yr
    , calendar.Fiscal_Month
    , calendar.Fiscal_Year_Qtr 
    , ISNULL(demand.cartridges, 0) cartridges
    , SUM(ISNULL(demand.imp_corrected_cartridges, 0)) OVER (PARTITION BY demand.platform_subset, demand.customer_engagement, baseprod.region_5, baseprod.country_alpha2, 
    baseprod.cal_date) AS sum_cartridges
    , ISNULL(demand.imp_corrected_cartridges, 0) imp_corrected_cartridges
    , ISNULL(baseprod.BaseProd_GRU, 0) BaseProd_GRU
    , ISNULL(baseprod.BaseProd_Contra_perUnit, 0) AS BaseProd_Contra_perUnit
    , ISNULL(baseprod.BaseProd_RevenueCurrencyHedge_Unit, 0) AS BaseProd_RevenueCurrencyHedge_Unit
    , (ISNULL(baseprod.BaseProd_GRU, 0) - ISNULL(baseprod.BaseProd_Contra_perUnit, 0) + ISNULL(baseprod.BaseProd_RevenueCurrencyHedge_Unit, 0)) AS
    BaseProd_NetRevenue_Unit
    , ISNULL(baseprod.BaseProd_VariableCost_perUnit, 0) BaseProd_VariableCost_perUnit
    , (ISNULL(baseprod.BaseProd_GRU, 0) - ISNULL(baseprod.BaseProd_Contra_perUnit, 0) + ISNULL(baseprod.BaseProd_RevenueCurrencyHedge_Unit, 0) - 
    ISNULL(baseprod.BaseProd_VariableCost_perUnit, 0)) AS BaseProd_CM_Unit
    , ISNULL(baseprod.BaseProd_FixedCost_perUnit, 0) BaseProd_FixedCost_perUnit
    , (ISNULL(baseprod.BaseProd_GRU, 0) - ISNULL(baseprod.BaseProd_Contra_perUnit, 0) + ISNULL(baseprod.BaseProd_RevenueCurrencyHedge_Unit, 0) - 
    ISNULL(baseprod.BaseProd_VariableCost_perUnit, 0) - ISNULL(baseprod.BaseProd_FixedCost_perUnit, 0)) AS BaseProd_GM_Unit
FROM 
    "fin_prod"."forecast_supplies_baseprod" baseprod 
    INNER JOIN
    "prod"."working_forecast_country" demand
        ON demand.base_product_number = baseprod.base_product_number 
        AND demand.cal_date = baseprod.cal_date 
        AND demand.country = baseprod.country_alpha2
        AND demand.version = '{dbutils.widgets.get('fin_ifs2_cart_version')}'
        AND baseprod.version  = '{dbutils.widgets.get('forecast_fin_version')}'
        AND demand.cal_date >= (SELECT cal_date FROM ifs2_02_filter_dates where record = 'FORECAST_SUPPLIES_BASEPROD')
        AND demand.cal_date <= '{dbutils.widgets.get('max_ifs2_date')}'
        AND EXISTS (
                    SELECT 1
                    FROM "stage".ifs2_qtr ifs2
                    WHERE demand.platform_subset = ifs2.platform_subset AND demand.country = ifs2.country_alpha2
                  )
     INNER JOIN "mdm"."calendar"
        ON calendar.date = baseprod.cal_date
), cartridge_mix (


SELECT
    platform_subset
    , customer_engagement
    , base_product_number
    , region_5
    , country_alpha2
    , cal_date
    , Fiscal_Year_Qtr
    , imp_corrected_cartridges
    , sum_cartridges
FROM 
  cartridge_financials
), platform_financials as (


SELECT 
    platform_subset
    , customer_engagement
    , region_5
    , country_alpha2
    , cal_date
    , Fiscal_Year_Qtr
    , SUM(imp_corrected_cartridges * BaseProd_NetRevenue_Unit) AS NetRevenue
    , SUM(imp_corrected_cartridges * BaseProd_CM_Unit) AS ContributionMargin
    , SUM(imp_corrected_cartridges * BaseProd_GM_Unit) AS GrossMargin
FROM
    cartridge_mix
GROUP BY
    platform_subset
    , customer_engagement
    , region_5
    , country_alpha2
    , cal_date
    , Fiscal_Year_Qtr
), ifs2_06_ib AS (


SELECT
    cal_date
    , country_alpha2 
    , [platform_subset]
    , [customer_engagement]
    , units
FROM
  "prod"."ib"
WHERE
    measure = 'IB'
    AND units >= 1
    AND version = '{dbutils.widgets.get("fin_ib_version")}'
    --and platform_subset = 'SELENE YET1 DM2'
    AND cal_date >= (SELECT cal_date FROM ifs2_filter_dates WHERE record = 'FORECAST_SUPPLIES_BASEPROD')    
)
SELECT
    'IFS2' as record
    ,(SELECT load_date FROM "prod"."version" WHERE record = 'ifs2'
            AND load_date = (SELECT MAX(load_date) FROM "prod"."version" WHERE record = 'ifs2')) AS load_date
    ,(SELECT version FROM "prod"."version" WHERE record = 'ifs2'
            AND version = (SELECT MAX(version) FROM "prod"."version" WHERE record = 'ifs2')) AS version
    , pf.platform_Subset
    , hardware_xref.pl
    , pf.customer_engagement
    , pf.region_5
    , pf.country_alpha2
    , country_xref.country
    , country_xref.region_3
    , country_xref.market10
    , pf.cal_date
    , pf.fiscal_Year_Qtr
    , ISNULL(hw_units, 0) AS hw_units
    , ISNULL(ifs2_netrevenue, 0) AS ifs2_netrevenue
    , ISNULL([IFS2_CM], 0) AS [IFS2_ContributionMargin]
    , ISNULL([IFS2_GM], 0) AS [IFS2_GrossMargin]
    , ISNULL(NetRevenue, 0) AS NetRevenue
    , ISNULL(ContributionMargin, 0) AS ContributionMargin
    , ISNULL(GrossMargin, 0) AS GrossMargin
    , ISNULL(units, 0) AS ib_units
    , cm.base_product_number
    , ISNULL(cm.imp_corrected_cartridges, 0) AS cartridges
    , ISNULL(cm.sum_cartridges, 0) AS sum_cartridges
    , ISNULL(BaseProd_GRU, 0) AS BaseProd_GRU
    , ISNULL(BaseProd_Contra_perUnit, 0) AS BaseProd_Contra_perUnit
    , ISNULL(BaseProd_RevenueCurrencyHedge_Unit, 0) AS BaseProd_RevenueCurrencyHedge_Unit
    , ISNULL(BaseProd_NetRevenue_Unit, 0) AS BaseProd_NetRevenue_Unit
    , ISNULL(BaseProd_VariableCost_perUnit, 0) AS BaseProd_VariableCost_perUnit
    , ISNULL(BaseProd_CM_Unit, 0) AS BaseProd_CM_Unit
    , ISNULL(BaseProd_FixedCost_perUnit, 0) AS BaseProd_FixedCost_perUnit
    , ISNULL(BaseProd_GM_Unit, 0) AS BaseProd_GM_Unit
FROM 
    platform_financials pf
    LEFT JOIN
    "stage"."ifs2_qtr" ifs2
        ON pf.platform_subset = ifs2.platform_subset
        AND UPPER(pf.customer_engagement) = UPPER(ifs2.customer_engagement)
        AMD pf.country_alpha2 = ifs2.country_alpha2
        AND pf.fiscal_year_qtr = ifs2.fiscal_year_qtr
    left join
    ifs2_06_ib ib
        ON ib.platform_subset = pf.platform_subset
        AND UPPER(ib.customer_engagement) = UPPER(pf.customer_engagement)
        AND ib.country = pf.country_alpha2
        AND ib.cal_date = pf.cal_date
    LEFT JOIN
    cartridge_mix cm
        ON cm.platform_subset = pf.platform_subset
        AND UPPER(cm.customer_engagement) = UPPER(pf.customer_engagement)
        AND cm.country_alpha2 = pf.country_alpha2
        AND cm.cal_date = pf.cal_date
    left join
    cartridge_financials cf
        ON cf.platform_subset = pf.platform_subset
        AND UPPER(cf.customer_engagement) = UPPER(pf.customer_engagement)
        AND cf.country_alpha2 = pf.country_alpha2
        AND cf.cal_date = pf.cal_date
        AND cf.base_product_number = cm.base_product_number
    LEFT JOIN
    "prod"."iso_country_code_xref" country_xref
        ON country_xref.country_alpha2 = pf.country_alpha2
    LEFT JOIN
    "prod"."hardware_xref" hardware_xref
        ON hardware_xref.platform_subset = pf.platform_subset

"""
query_list.append(["stage.ifs2_report", ifs2_promotion, "overwrite"])

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
