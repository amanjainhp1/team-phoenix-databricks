# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, current_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType, DecimalType

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# check list_price_EU_CountryList.currency
query = """
CREATE OR REPLACE VIEW financials.v_base_GRU_Reconciliation AS 

WITH 
       forecast_list AS
             (
                    select DISTINCT  base_product_number
                           , base_product_line_code AS pl
                           , country_alpha2
                    FROM 
                           fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
                    WHERE 
                           version = (SELECT MAX(version) from fin_prod.forecast_supplies_baseprod)
                           and
                           insights_base_units >= 1
                           /*and                      
                           cal_date = (select min(cal_date) from fin_prod.forecast_supplies_baseprod
                                               WHERE
                                        version = (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod)) */
             ),
       composite_key AS
             (
                    SELECT base_product_number
                           , pl
                           , country_alpha2
                        , base_product_number + ' ' + pl + ' ' + country_alpha2 AS composite_key
                    FROM forecast_list
             ),

       actuals AS
             (
                    SELECT DISTINCT
                           cal_date
                           , iso_country_code_xref.region_5
                           , actuals_supplies_baseprod.country_alpha2
                           , CASE 
                                 WHEN list_price_EU_Country_List.currency = 'EURO' THEN 'EUR'
                                 WHEN list_price_EU_Country_List.currency = 'DOLLAR' THEN 'USD'
                                 WHEN actuals_supplies_baseprod.country_alpha2 = 'CL' THEN 'USD'
                                 WHEN actuals_supplies_baseprod.country_alpha2 = 'PR' THEN 'USD'
                                 ELSE country_currency_map_landing.currency_iso_code 
                          END AS currency
                           , base_product_number
                           , pl
                           , SUM(gross_revenue)/NULLIF(SUM(revenue_units),0) base_gru 
                           , SUM(revenue_units) revenue_units
                           , SUM(gross_revenue) gross_revenue
                           , base_product_number + ' ' + pl + ' ' + actuals_supplies_baseprod.country_alpha2 AS product_key 
                    FROM 
                           fin_prod.actuals_supplies_baseprod actuals_supplies_baseprod
                           LEFT JOIN 
                           mdm.iso_country_code_xref iso_country_code_xref
                                 ON actuals_supplies_baseprod.country_alpha2 = iso_country_code_xref.country_alpha2
                           LEFT JOIN
                           mdm.list_price_EU_Country_List list_price_EU_Country_List
                                 ON list_price_EU_Country_List.country_alpha2 = actuals_supplies_baseprod.country_alpha2
                           LEFT JOIN 
                           mdm.country_currency_map country_currency_map_landing
                                 ON country_currency_map_landing.country_alpha2 = actuals_supplies_baseprod.country_alpha2
                    WHERE 
                           (cal_date > (SELECT DATEADD(MONTH, -6, MAX(cal_date)) FROM fin_prod.actuals_supplies_baseprod))
                           and revenue_units > 0
                           /*
                           and actuals_supplies_baseprod.version = (select max(version) from fin_prod.actuals_supplies_baseprod)
                           AND pl IN (select distinct pl from fin_prod.forecast_supplies_baseprod 
                                        where version = (select max(version) from fin_prod.forecast_supplies_baseprod))
                           */
                    GROUP BY 
                           cal_date
                           , iso_country_code_xref.region_5
                           , actuals_supplies_baseprod.country_alpha2
                           , currency
                           , currency_iso_code
                           , base_product_number
                           , pl
             )

       , actuals_acct AS 
             (
                    SELECT DISTINCT
                           'ACTUALS' record_type
                           , cal_date
                           , supplies_xref.technology
                           , actuals.base_product_number
                           , supplies_xref.cartridge_alias
                           , actuals.pl base_product_line_code
                           , region_5
                           , country_alpha2
                           , revenue_units insights_base_units
                           , base_gru
                           , gross_revenue
                           , currency
                           , acct_rates.accountingrate 
                    FROM 
                           actuals actuals
                           LEFT JOIN
                           prod.acct_rates acct_rates
                                 ON acct_rates.isocurrcd = actuals.currency AND acct_rates.effectivedate = actuals.cal_date
                           LEFT JOIN
                           mdm.supplies_xref supplies_xref
                           ON supplies_xref.base_product_number = actuals.base_product_number
                           WHERE 1=1
                                 AND product_key IN (SELECT distinct composite_key FROM composite_key)
             )

       , forecast AS 
             (
                    select DISTINCT
                           base_product_number
                           , base_product_line_code
                           , forecast_supplies_baseprod.region_5
                           , forecast_supplies_baseprod.country_alpha2
                           /*, SUM(Insights_Base_Units) insights_base_units
                           , SUM(BaseProd_GRU*Insights_Base_Units)/NULLIF(SUM(Insights_Base_Units),0) base_gru
                           , SUM(BaseProd_GRU*Insights_Base_Units) gross_revenue
                           , MIN(cal_date) as cal_date*/
                           , insights_base_units
                           , BaseProd_GRU as base_gru
                           , (BaseProd_GRU*Insights_Base_Units) as gross_revenue
                           , cal_date
                           , CASE 
                                 WHEN list_price_EU_Country_List.currency = 'EURO' THEN 'EUR'
                                 WHEN list_price_EU_Country_List.currency = 'DOLLAR' THEN 'USD'
                                 WHEN forecast_supplies_baseprod.country_alpha2 = 'CL' THEN 'USD'
                                 WHEN forecast_supplies_baseprod.country_alpha2 = 'PR' THEN 'USD'
                                 ELSE country_currency_map_landing.currency_iso_code 
                           END as currency
                    FROM 
                           fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
                           LEFT JOIN 
                           mdm.list_price_eu_country_list list_price_EU_Country_List
                                 ON list_price_EU_Country_List.country_alpha2 = forecast_supplies_baseprod.country_alpha2
                           LEFT JOIN 
                           mdm.country_currency_map country_currency_map_landing
                                 ON country_currency_map_landing.country_alpha2 = forecast_supplies_baseprod.country_alpha2
                    WHERE 
                           version = (SELECT MAX(version) from fin_prod.forecast_supplies_baseprod)
                           and
                           Insights_Base_Units >= 1
                           /*and
                           cal_date <= (SELECT DATEADD(MONTH, 18, MIN(cal_date)) FROM fin_prod.forecast_supplies_baseprod 
                                 WHERE
                                        version = (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod))
                           cal_date = (select min(cal_date) from fin_prod.forecast_supplies_baseprod
                                               WHERE
                                        version = (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod))*/
                           
                    /*GROUP BY 
                           base_product_number
                           , base_product_line_code
                           , forecast_supplies_baseprod.region_5
                           , forecast_supplies_baseprod.country_alpha2
                           , currency_iso_code
                           , currency*/
             )
       , forecast_acct AS
             (
                    SELECT DISTINCT
                           'FORECAST' record_type
                           , cal_date
                           , supplies_xref.technology
                           , forecast.base_product_number
                           , supplies_xref.cartridge_alias
                           , base_product_line_code
                           , region_5
                           , country_alpha2
                           /*, avg(Insights_Base_Units) insights_base_units
                           , avg(Base_GRU) base_gru
                           , avg(gross_revenue) gross_revenue*/
                           , insights_base_units
                           , base_gru
                           , gross_revenue
                           , currency
                           , acct_rates.accountingrate
                    FROM 
                           forecast
                           LEFT JOIN 
                           prod.acct_rates acct_rates
                                 ON acct_rates.IsoCurrCd = forecast.Currency
                           LEFT JOIN mdm.supplies_xref supplies_xref
                                 ON supplies_xref.base_product_number = forecast.base_product_number
                    where 
                          acct_rates.effectivedate = (SELECT MAX(effectivedate) FROM prod.acct_rates acct_rates)
                    /*group by
                           cal_date
                           , technology
                           , forecast.base_product_number
                           , cartridge_alias
                           , base_product_line_code
                           , region_5
                           , country_alpha2
                           , currency
                           , acct_rates.accountingrate*/
                    

             ) 
  
       SELECT DISTINCT
             record_type
             , cal_date
             , technology
             , base_product_number
             , cartridge_alias as fiji_alias
             , base_product_line_code
             , region_5
             , country_alpha2
             , insights_base_units
             , base_gru
             , gross_revenue
             , currency
             , accountingrate
             , (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod) AS version
       FROM
             actuals_acct
       UNION
       SELECT DISTINCT
             record_type
             , cal_date
             , technology
             , base_product_number
             , cartridge_alias as fiji_alias
             , base_product_line_code
             , region_5
             , country_alpha2
             , insights_base_units
             , base_gru
             , gross_revenue
             , currency
             , accountingrate
             , (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod) AS version
       FROM
             forecast_acct;


      
GRANT ALL ON TABLE financials.v_base_GRU_Reconciliation TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

# check IE2_PROD.dbo.hw_product_family_ink_business_segments
query = """
CREATE OR REPLACE VIEW financials.v_act_plus_fcst_financials AS 


--- open supplies actuals base product level of detail
WITH
       current_fiscal_yr AS
       (
             SELECT 
                    fiscal_yr-4 start_fiscal_Yr
                    , fiscal_yr+5 end_fiscal_Yr
             FROM
             mdm.calendar
             WHERE Date = CONVERT(DATE, GETDATE())
       ),
    cat_group AS 
       (
             SELECT distinct 
                shm.base_product_number,
             --  hw.hw_product_family as tech_split,
                seg.technology_type
             FROM mdm.supplies_hw_mapping shm
             LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset=shm.platform_subset
             LEFT JOIN IE2_PROD.dbo.hw_product_family_ink_business_segments seg on seg.hw_product_family=hw.hw_product_family
             WHERE hw.technology IN ('ink','pwa') AND shm.customer_engagement ='Trad' and shm.official = 1
             and seg.technology_type is not null
       ),
       supplies_baseprod_actuals AS
       (
             SELECT 
                    'actuals' AS record_type
                    , actuals_supplies_baseprod.base_product_number
                    , actuals_supplies_baseprod.pl base_product_line_code
                    , supplies_xref.Technology
                    , CASE 
                           WHEN (actuals_supplies_baseprod.pl in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')) THEN 1
                           ELSE 0
                      END AS IsCanon
                    , iso_country_code_xref.region_5
                    , iso_country_code_xref.country_alpha2
                    , iso_country_code_xref.market10
                    , cal_date
                    , calendar.Fiscal_Year_Qtr
                    , calendar.Fiscal_Year_Half
                    , calendar.Fiscal_Yr
                    , supplies_xref.k_color
                    , supplies_xref.size
                    , supplies_xref.supplies_family
                    , SUM(revenue_units) Units
                    , SUM(gross_revenue) AS GrossRevenue
                    , SUM(contractual_discounts) + SUM(discretionary_discounts) AS Contra
                    , SUM(net_currency) AS Revenue_Currency
                    , SUM(net_revenue) AS NetRevenue
                    , SUM(total_cos) AS Total_COS
                    , SUM(gross_profit) AS GrossProfit
             FROM IE2_Financials.dbo.actuals_supplies_baseprod actuals_supplies_baseprod
             LEFT JOIN
             IE2_Prod.dbo.supplies_xref supplies_xref
                    ON supplies_xref.base_product_number = actuals_supplies_baseprod.base_product_number
             LEFT JOIN
             IE2_Prod.dbo.iso_country_code_xref iso_country_code_xref
                    ON iso_country_code_xref.country_alpha2 = actuals_supplies_baseprod.country_alpha2
             LEFT JOIN
             IE2_Prod.dbo.calendar calendar
                    ON calendar.Date = actuals_supplies_baseprod.cal_date
             CROSS JOIN
             current_fiscal_yr
             WHERE Fiscal_Yr >= Start_Fiscal_Yr
             and customer_engagement <> 'est_direct_fulfillment'
             and Day_of_Month = 1
             GROUP BY actuals_supplies_baseprod.record
                    , actuals_supplies_baseprod.base_product_number
                    , actuals_supplies_baseprod.pl 
                    , supplies_xref.Technology
                    , iso_country_code_xref.region_5
                    , iso_country_code_xref.country_alpha2
                    , iso_country_code_xref.market10
                    , cal_date
                    , calendar.Fiscal_Year_Qtr
                    , calendar.Fiscal_Year_Half
                    , calendar.Fiscal_Yr
                    , supplies_xref.k_color
                    , supplies_xref.size
                    , supplies_xref.supplies_family
       )
       --select distinct cal_date, fiscal_Yr from supplies_baseprod_actuals
       ,
--- open supplies forecast at the base product level of detail, convert to total values from per unit entries

       supplies_baseprod_forecast AS  --(from landing; after alpha release, update to financials db)
       (
             SELECT 
                    'forecast' AS record_type
                    , forecast_supplies_baseprod.base_product_number 
                    , base_product_line_code 
                    , supplies_xref.Technology
                    , CASE 
                           WHEN (base_product_line_code in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')) THEN 1
                           ELSE 0
                      END AS IsCanon
                    , iso_country_code_xref.region_5
                    , iso_country_code_xref.country_alpha2
                    , iso_country_code_xref.market10
                    , cal_date
                    , calendar.Fiscal_Year_Qtr
                    , calendar.Fiscal_Year_Half
                    , calendar.Fiscal_Yr
                    , supplies_xref.k_color
                    , supplies_xref.size
                    , supplies_xref.supplies_family
                    , coalesce(Insights_Base_Units, 0) Units
                    , coalesce(BaseProd_GRU, 0) * coalesce(Insights_Base_Units, 0) GrossRevenue
                    , coalesce(BaseProd_Contra_perUnit, 0) * coalesce(Insights_Base_Units, 0) Contra
                    , coalesce(BaseProd_RevenueCurrencyHedge_Unit, 0) * coalesce(Insights_Base_Units, 0) Revenue_Currency
                    , ((coalesce(BaseProd_GRU, 0) - coalesce(BaseProd_Contra_perUnit, 0) + coalesce(BaseProd_RevenueCurrencyHedge_Unit, 0))*coalesce(Insights_Base_Units, 0)) NetRevenue
                    , ((coalesce(BaseProd_VariableCost_perUnit, 0) + coalesce(BaseProd_FixedCost_perUnit, 0))* coalesce(Insights_Base_Units, 0)) Total_COS
                    , ((coalesce(BaseProd_GRU, 0) - coalesce(BaseProd_Contra_perUnit, 0) + coalesce(BaseProd_RevenueCurrencyHedge_Unit, 0) - coalesce(BaseProd_VariableCost_perUnit, 0) - coalesce(BaseProd_FixedCost_perUnit, 0))*coalesce(Insights_Base_Units, 0)) GrossProfit
             FROM IE2_Financials.dbo.forecast_supplies_baseprod forecast_supplies_baseprod
             left join
             ie2_prod.dbo.iso_country_code_xref iso_country_code_xref
             on forecast_supplies_baseprod.country_alpha2 = iso_country_code_xref.country_alpha2
             left join
             ie2_prod.dbo.calendar calendar
             on calendar.Date = forecast_supplies_baseprod.cal_date
             left join
             ie2_prod.dbo.supplies_xref supplies_xref
             on supplies_xref.base_product_number = forecast_supplies_baseprod.base_product_number
             CROSS JOIN
             current_fiscal_yr
             WHERE Fiscal_Yr <= End_Fiscal_Yr
             and Day_of_Month = 1
             and forecast_supplies_baseprod.version = (select max(version) from IE2_Financials.dbo.forecast_supplies_baseprod)
             and forecast_supplies_baseprod.cal_date > (select max(cal_date) from IE2_Financials.dbo.actuals_supplies_baseprod)

       )
--     SELECT [base_product_line_code]
--      ,[Fiscal_Year_Qtr]
--      ,sum([NetRevenue]) as netRevenue
--      ,sum([GrossProfit]) as GrossProfit
--  FROM supplies_baseprod_forecast
--  where base_product_line_code = '1N'
--  group by [base_product_line_code]
--      ,[Fiscal_Year_Qtr]
--order by 1,2;
       ,
       
       supplies_actuals_join_forecast AS
       (
             SELECT record_type
                    , base_product_number
                    , base_product_line_code
                    , Technology
                    , IsCanon
                    , region_5
                    , country_alpha2
                    , market10
                    , cal_date
                    , Fiscal_Year_Qtr
                    , Fiscal_Year_Half
                    , Fiscal_Yr
                    , k_color
                    , size
                    , supplies_family
                    , Units
                    , GrossRevenue
                    , Contra
                    , Revenue_Currency
                    , NetRevenue
                    , Total_COS
                    , GrossProfit
             FROM supplies_baseprod_actuals
             UNION ALL
             SELECT record_type
                    , base_product_number
                    , base_product_line_code
                    , Technology
                    , IsCanon
                    , region_5
                    , country_alpha2
                    , market10
                    , cal_date
                    , Fiscal_Year_Qtr
                    , Fiscal_Year_Half
                    , Fiscal_Yr
                    , k_color
                    , size
                    , supplies_family
                    , Units
                    , GrossRevenue
                    , Contra
                    , Revenue_Currency
                    , NetRevenue
                    , Total_COS
                    , GrossProfit
             FROM supplies_baseprod_forecast
       )
             SELECT fcst.record_type
                    , fcst.base_product_number
                    --, cat_group.technology_type
                    , rdma.base_prod_desc
                    , fcst.base_product_line_code
                    , fcst.Technology
                    , fcst.IsCanon
                    , fcst.region_5
                    , fcst.country_alpha2
                    , fcst.market10
                    , fcst.cal_date
                    , fcst.Fiscal_Year_Qtr
                    , fcst.Fiscal_Year_Half
                    , fcst.Fiscal_Yr
                    , fcst.k_color
                    , fcst.size
                    , fcst.supplies_family
                    , fcst.Units
                    , fcst.GrossRevenue
                    , fcst.Contra
                    , fcst.Revenue_Currency
                    , fcst.NetRevenue
                    , fcst.Total_COS
                    , fcst.GrossProfit
             --INTO IE2_Landing.compare.IE_Financials
             FROM supplies_actuals_join_forecast fcst
             LEFT JOIN cat_group on cat_group.base_product_number = fcst.base_product_number
             LEFT JOIN IE2_Prod.dbo.rdma rdma on rdma.base_prod_number = fcst.base_product_number
             ;



      
GRANT ALL ON TABLE financials.v_act_plus_fcst_financials TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

query = """
CREATE OR REPLACE VIEW financials.v_stf_dollarization_cycle_over_cycle AS 

SELECT
       record
       ,geography
       ,base_product_number
       ,pl
       ,UPPER(Technology) AS technology
       ,cal_date
       ,units
       ,equivalent_units
       ,gross_revenue
       ,contra
       ,revenue_currency_hedge
       ,net_revenue
       ,variable_cost
       ,contribution_margin
       ,fixed_cost
       ,gross_margin
       ,insights_units
       ,version
       ,load_date
       ,username
FROM fin_prod.stf_dollarization

UNION ALL

SELECT 
    'SUPPLIES_STF' as record
    ,geography
       ,base_product_number
       ,pl
       ,UPPER(Technology) AS technology
       ,cal_date
       ,units
       ,equivalent_units
       ,gross_revenue
       ,contra
       ,revenue_currency_hedge
       ,net_revenue
       ,variable_cost
       ,contribution_margin
       ,fixed_cost
       ,gross_margin
       ,insights_units
    ,'WORKING VERSION' as version
    ,GETDATE() as load_date
    ,username
FROM financials.v_stf_dollarization;



      
GRANT ALL ON TABLE financials.v_stf_dollarization_cycle_over_cycle TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

query = """
CREATE OR REPLACE VIEW financials.v_stf_GRU AS 

SELECT 
       base_product_number
       , base_product_line_code
       ,region_5
       ,cal_date
       ,baseprod_gru
       , 'WORKING' as version
FROM fin_prod.forecast_supplies_baseprod_region_stf
WHERE 1=1 
       --AND version = (SELECT max(version) FROM fin_prod.forecast_supplies_baseprod_region_stf)
       /*AND EXISTS 
       (
             SELECT
                    base_product_number
                    ,region_5
                    ,cal_date 
             --FROM stage.supplies_stf_landing
             FROM fin_prod.stf_dollarization
             WHERE 1=1
                    --AND supplies_stf_landing.base_product_number = forecast_supplies_baseprod_region.base_product_number
                    --AND supplies_stf_landing.geography = forecast_supplies_baseprod_region.region_5
                    --AND supplies_stf_landing.cal_date = forecast_supplies_baseprod_region.cal_date
                    AND stf_dollarization.base_product_number = forecast_supplies_baseprod_region.base_product_number
                    AND stf_dollarization.geography = forecast_supplies_baseprod_region.region_5
                    AND stf_dollarization.cal_date = forecast_supplies_baseprod_region.cal_date
                    AND version = (SELECT MAX(version) FROM fin_prod.stf_dollarization)
       )*/

       UNION

       SELECT 
             base_product_number
             , pl as base_product_line_code
             ,geography as region_5
             ,cal_date
             ,(gross_revenue/nullif(Units,0)) baseprod_gru
             ,version
       FROM fin_prod.stf_dollarization
;

GRANT ALL ON TABLE financials.v_stf_GRU TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

query = """
CREATE OR REPLACE VIEW financials.v_stf_units_cycle_over_cycle AS 
(
SELECT
       record
       ,geography
       ,base_product_number
       ,pl
       ,UPPER(Technology) AS technology
       ,cal_date
       ,units
       ,version
       ,load_date
       ,username
FROM fin_prod.stf_dollarization

UNION ALL

SELECT 
    'SUPPLIES_STF' as record
    ,geography
      ,base_product_number
      ,pl
      ,UPPER(Technology) AS technology
      ,cal_date
      ,units
    ,'WORKING VERSION' as version
    ,GETDATE() as load_date
    ,username
FROM financials.v_stf_dollarization
);

GRANT ALL ON TABLE financials.v_stf_units_cycle_over_cycle TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------

#check view
query = """

CREATE OR REPLACE VIEW financials.v_finance_forecast_PL AS

       WITH Base_PL as
       (
             SELECT 
                    'BASE_PRODUCT' record_type
                    , forecast_supplies_baseprod.base_product_number product_number
                    , base_product_line_code product_line_code
                    , supplies_xref.technology
                    , supplies_xref.supplies_family
                    , CASE 
                           WHEN (base_product_line_code in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')) THEN 1
                           ELSE 0
                      END AS is_canon
                    , iso_country_code_xref.region_5
                    , iso_country_code_xref.country_alpha2
                    , iso_country_code_xref.market10
                    , cal_date
                    , calendar.fiscal_year_qtr
                    , calendar.fiscal_year_half
                    , calendar.fiscal_yr
                    , insights_base_units units
                    , coalesce(baseprod_gru, 0) * coalesce(insights_base_units, 0) gross_revenue
                    , coalesce(baseprod_contra_per_unit, 0) * coalesce(insights_base_units, 0) contra
                    , coalesce(baseprod_revenue_currency_hedge_unit, 0) * coalesce(insights_base_units, 0) revenue_currency
                    , ((coalesce(baseprod_gru, 0) - coalesce(baseprod_contra_per_unit, 0) + coalesce(baseprod_revenue_currency_hedge_unit, 0))*coalesce(insights_base_units, 0)) net_revenue
                    , coalesce(baseprod_variable_cost_per_unit, 0) * coalesce(insights_base_units, 0) variable_cost
                    , ((coalesce(baseprod_gru, 0) - coalesce(baseprod_contra_per_unit, 0) + coalesce(baseprod_revenue_currency_hedge_unit, 0) - coalesce(baseprod_variable_cost_per_unit, 0))*coalesce(insights_base_units, 0)) contribution_margin
                    , coalesce(baseprod_fixed_cost_per_unit, 0) * coalesce(insights_base_units, 0) fixed_cost
                    , ((coalesce(baseprod_gru, 0) - coalesce(baseprod_contra_per_unit, 0) + coalesce(baseprod_revenue_currency_hedge_unit, 0) - coalesce(baseprod_variable_cost_per_unit, 0) - coalesce(baseprod_fixed_cost_per_unit, 0))*coalesce(insights_base_units, 0)) gross_profit
                    , forecast_supplies_baseprod.contra_version
                    , forecast_supplies_baseprod.variable_cost_version
                    , lpv.acct_rates_version
                    , lpv.lp_gpsy_version
             FROM fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
             inner join
             mdm.iso_country_code_xref iso_country_code_xref
             on forecast_supplies_baseprod.country_alpha2 = iso_country_code_xref.country_alpha2
             and forecast_supplies_baseprod.version = (select max(version) from fin_prod.forecast_supplies_baseprod)
             inner join
             mdm.calendar calendar
             on calendar.Date = forecast_supplies_baseprod.cal_date
             inner join
             mdm.supplies_xref supplies_xref
             on supplies_xref.base_product_number = forecast_supplies_baseprod.base_product_number
             inner join
             fin_prod.list_price_version lpv
             on lpv.version = forecast_supplies_baseprod.sales_gru_version
                    
       ) 
       ,
       cartridge_printer_mix AS
       (
             SELECT 
                    sup.cal_date,
                    geography AS market10,
                    sup.platform_subset,
                    base_product_number,
                    CASE
                           WHEN SUM(adjusted_cartridges) OVER (PARTITION BY cal_date, geography, base_product_number) = 0 THEN NULL
                           ELSE adjusted_cartridges / SUM(adjusted_cartridges) OVER (PARTITION BY cal_date, geography, base_product_number)
                    END AS cartridge_printer_mix 
             FROM prod.working_forecast sup
             WHERE version = (select max(version) from prod.working_forecast)
             AND sup.cal_date >= (SELECT MIN(cal_date) FROM fin_prod.forecast_supplies_baseprod) 
             AND adjusted_cartridges <> 0
             AND geography_grain = 'MARKET10'
             GROUP BY sup.cal_date, geography, sup.platform_subset, base_product_number, adjusted_cartridges
       )
       
             SELECT 
                    record_type
                    ,  product_number
                    ,  product_line_code
                    , base_pl.Technology
                    , is_canon
                    , mix.platform_subset
                    , hw_product_family
                    , supplies_family
                    , region_5
                    , country_alpha2
                    , base_pl.market10
                    , base_pl.cal_date
                    , fiscal_year_qtr
                    , fiscal_year_half
                    , fiscal_yr
                    , coalesce(units, 0) * coalesce(cartridge_printer_mix, 0) units
                    , coalesce(gross_revenue, 0) * coalesce(cartridge_printer_mix, 0) gross_revenue
                    , coalesce(contra, 0) * coalesce(cartridge_printer_mix, 0) contra
                    , coalesce(revenue_currency, 0) * coalesce(cartridge_printer_mix, 0) revenue_currency
                    , coalesce(net_revenue, 0) * coalesce(cartridge_printer_mix, 0) net_revenue
                    , coalesce(variable_cost, 0) * coalesce(cartridge_printer_mix, 0) variable_cost
                    , coalesce(contribution_margin, 0) * coalesce(cartridge_printer_mix, 0) contribution_margin
                    , coalesce(fixed_cost, 0) * coalesce(cartridge_printer_mix, 0) fixed_cost
                    , coalesce(gross_profit, 0) * coalesce(cartridge_printer_mix, 0) gross_profit
                    , contra_version
                    , variable_cost_version
                    , acct_rates_version
                    , lp_gpsy_version
             FROM Base_PL base_pl
             inner join cartridge_printer_mix mix
                    on base_pl.cal_date = mix.cal_date
                    and base_pl.market10 = mix.market10
                    and product_number = base_product_number
             inner join mdm.hardware_xref hw 
                    on mix.platform_subset = hw.platform_subset;


GRANT ALL ON TABLE financials.v_finance_forecast_PL TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query)

# COMMAND ----------


