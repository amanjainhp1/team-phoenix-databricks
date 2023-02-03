-- ifs2.ifs2_report_vw source

CREATE VIEW ifs2.ifs2_report_vw AS 


WITH ifs2 as
(
SELECT 
    platform_subset
    , customer_engagement
    , country_alpha2
    , fiscal_year_qtr
    , hw_units
    , ifs2_net_revenue
    , ifs2_contribution_margin
    , ifs2_gross_margin
FROM 
    stage.ifs2_qtr_landing
    WHERE version = (SELECT MAX(version) FROM stage.ifs2_qtr_landing)
)
, cartridge_financials as
(
SELECT DISTINCT 
    demand.platform_subset
    , demand.customer_engagement
    , baseprod.base_product_number
    , baseprod.region_5
    , baseprod.country_alpha2
    , baseprod.cal_date
    , calendar.fiscal_yr
    , calendar.fiscal_month
    , calendar.fiscal_year_qtr 
    , COALESCE(demand.cartridges, 0) cartridges
    , SUM(COALESCE(demand.imp_corrected_cartridges, 0)) OVER (PARTITION BY demand.platform_subset, demand.customer_engagement, baseprod.region_5, baseprod.country_alpha2, baseprod.cal_date) AS sum_cartridges
    , COALESCE(demand.imp_corrected_cartridges, 0) imp_corrected_cartridges
    , COALESCE(baseprod.baseprod_gru, 0) baseprod_gru
    , COALESCE(baseprod.baseprod_contra_per_unit, 0) AS baseprod_contra_per_unit
    , COALESCE(baseprod.baseprod_revenue_currency_hedge_unit, 0) AS baseprod_revenue_currency_hedge_unit
    , (COALESCE(baseprod.baseprod_gru, 0) - COALESCE(baseprod.baseprod_contra_per_unit, 0) + COALESCE(baseprod.baseprod_revenue_currency_hedge_unit, 0)) AS baseprod_net_revenue_unit
    , COALESCE(baseprod.baseprod_variable_cost_per_unit, 0) baseprod_variable_cost_per_unit
    , (COALESCE(baseprod.baseprod_gru, 0) - COALESCE(baseprod.baseprod_contra_per_unit, 0) + COALESCE(baseprod.baseprod_revenue_currency_hedge_unit, 0) - COALESCE(baseprod.baseprod_variable_cost_per_unit, 0)) AS baseprod_cm_unit
    , COALESCE(baseprod.baseprod_fixed_cost_per_unit, 0) baseprod_fixed_cost_per_unit
    , (COALESCE(baseprod.baseprod_gru, 0) - COALESCE(baseprod.baseprod_contra_per_unit, 0) + COALESCE(baseprod.baseprod_revenue_currency_hedge_unit, 0) - COALESCE(baseprod.baseprod_variable_cost_per_unit, 0) - COALESCE(baseprod.baseprod_fixed_cost_per_unit, 0)) AS baseprod_gm_unit
FROM 
fin_prod.forecast_supplies_baseprod baseprod 
inner join
prod.working_forecast_country demand
    ON demand.base_product_number = baseprod.base_product_number 
    AND demand.cal_date = baseprod.cal_date 
    AND demand.country = baseprod.country_alpha2
inner join
mdm.calendar calendar
    ON calendar.date = baseprod.cal_date
WHERE 
baseprod.version = (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod)
    AND demand.version = (SELECT MAX(version) FROM prod.working_forecast_country)
AND demand.cal_date >= (SELECT min(cal_date) FROM fin_prod.forecast_supplies_baseprod baseprod WHERE baseprod.version = (SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod))
)
, cartridge_mix as
(
SELECT
    platform_subset
    , customer_engagement
    , base_product_number
    , region_5
    , country_alpha2
    , cal_date
    , fiscal_year_qtr
    , imp_corrected_cartridges
    , sum_cartridges
FROM 
    cartridge_financials
)
, platform_financials as
(
SELECT 
    platform_subset
    , customer_engagement
    , region_5
    , country_alpha2
    , cal_date
    , fiscal_year_qtr
    , SUM(imp_corrected_cartridges * baseprod_net_revenue_unit) AS net_revenue
    , SUM(imp_corrected_cartridges * baseprod_cm_unit) AS contribution_margin
    , SUM(imp_corrected_cartridges * baseprod_gm_unit) AS gross_margin
from
    cartridge_financials
group by
    platform_subset
    , customer_engagement
    , region_5
    , country_alpha2
    , cal_date
    , fiscal_year_qtr
)
, ib AS
(
SELECT
    platform_subset
    , customer_engagement
    , country_alpha2
    , cal_date
    , units
FROM
prod.ib
WHERE
    version = (SELECT MAX(version) FROM prod.ib)
    AND measure = 'IB'
    AND customer_engagement = 'TRAD'
)
, ifs2_report_data AS
(
SELECT 
    pf.platform_Subset
    , hardware_xref.pl
    , hardware_xref.technology
    , hardware_xref.hw_product_family
    , pf.customer_engagement
    , pf.region_5
    , pf.country_alpha2
    , country_xref.country
    , country_xref.region_3
    , country_xref.market10
    , pf.cal_date
    , pf.fiscal_year_qtr
    , COALESCE(hw_units, 0) AS hw_units
    , COALESCE(ifs2_net_revenue, 0) AS ifs2_net_revenue
    , COALESCE(ifs2_contribution_margin, 0) AS ifs2_contribution_margin
    , COALESCE(ifs2_gross_margin, 0) AS ifs2_gross_margin
    , COALESCE(net_revenue, 0) AS net_revenue
    , COALESCE(contribution_margin, 0) AS contribution_margin
    , COALESCE(gross_margin, 0) AS gross_margin
    , COALESCE(units, 0) AS ib_units
    , cm.base_product_number
    , COALESCE(cm.imp_corrected_cartridges, 0) AS cartridges
    , COALESCE(cm.sum_cartridges, 0) AS sum_cartridges
    , COALESCE(baseprod_gru, 0) AS baseprod_gru
    , COALESCE(baseprod_contra_per_unit, 0) AS baseprod_contra_per_unit
    , COALESCE(baseprod_revenue_currency_hedge_unit, 0) AS baseprod_revenue_currency_hedge_unit
    , COALESCE(baseprod_net_revenue_unit, 0) AS baseprod_net_revenue_unit
    , COALESCE(baseprod_variable_cost_per_unit, 0) AS baseprod_variable_cost_per_unit
    , COALESCE(baseprod_cm_unit, 0) AS baseprod_cm_unit
    , COALESCE(baseprod_fixed_cost_per_unit, 0) AS baseprod_fixed_cost_per_unit
    , COALESCE(baseprod_gm_unit, 0) AS baseprod_gm_unit
    , (SELECT MAX(version) FROM prod.working_forecast_country) AS version
FROM 
    platform_financials pf
    LEFT JOIN
    ifs2 ifs2
        ON pf.platform_subset = ifs2.platform_subset
        AND pf.customer_engagement = ifs2.customer_engagement
        AND pf.country_alpha2 = ifs2.country_alpha2
        AND pf.fiscal_year_qtr = ifs2.fiscal_year_qtr
    LEFT JOIN
    ib ib
        ON ib.platform_subset = pf.platform_subset
        AND ib.customer_engagement = pf.customer_engagement
        AND ib.country_alpha2 = pf.country_alpha2
        AND ib.cal_date = pf.cal_date
    LEFT JOIN
    cartridge_mix cm
        ON cm.platform_subset = pf.platform_subset
        AND cm.customer_engagement = pf.customer_engagement
        AND cm.country_alpha2 = pf.country_alpha2
        AND cm.cal_date = pf.cal_date
    LEFT JOIN
    cartridge_financials cf
        ON cf.platform_subset = pf.platform_subset
        AND cf.customer_engagement = pf.customer_engagement
        AND cf.country_alpha2 = pf.country_alpha2
        AND cf.cal_date = pf.cal_date
        AND cf.base_product_number = cm.base_product_number
    LEFT JOIN
    mdm.iso_country_code_xref country_xref
        ON country_xref.country_alpha2 = pf.country_alpha2
    LEFT JOIN
    mdm.hardware_xref hardware_xref
        ON hardware_xref.platform_subset = pf.platform_subset

)
SELECT * FROM ifs2_report_data;