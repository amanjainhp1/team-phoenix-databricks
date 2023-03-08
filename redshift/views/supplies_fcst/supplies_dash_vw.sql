-- supplies_fcst.supplies_dash_vw source
CREATE OR REPLACE VIEW supplies_fcst.supplies_dash_vw AS WITH mdate as (
        SELECT MAX(cal_date) as max_date
        FROM prod.norm_shipments
        WHERE version = (
                SELECT MAX(version)
                FROM prod.norm_shipments
            )
    ),
    tech as (
        SELECT distinct pl
        FROM mdm.product_line_scenarios_xref
        WHERE pl_level_1 IN ('INK', 'PWA', 'LASER')
    ),
    yr_range as (
        SELECT fiscal_yr - 5 start_year,
            fiscal_yr + 5 end_year
        FROM mdm.calendar
        WHERE cast(date as date) = cast(getdate() as date)
    ),
    dt_list as (
        SELECT DISTINCT date
        FROM mdm.calendar c
            LEFT JOIN yr_range ys on 1 = 1
        WHERE c.Day_of_Month = 1
            and c.fiscal_yr between ys.start_year and ys.end_year
    ),
    matures as (
        SELECT DISTINCT hw_product_family,
            CASE
                WHEN Business_Segment1 = '6_MATURES' THEN 'MATURE'
                ELSE 'NON-MATURE'
            END AS m_nm
        FROM mdm.hw_product_family_ink_business_segments
        WHERE hw_product_family IS NOT NULL
    ),
    biz_segments AS (
        SELECT DISTINCT hw_product_family,
            customer_engagement,
            Business_Segment1,
            Business_Segment2,
            Business_Segment3
        FROM mdm.hw_product_family_ink_business_segments
        WHERE hw_product_family IS NOT NULL
    ),
    usage_share1 as (
        SELECT distinct us.record,
            us.version,
            us.platform_subset,
            us.customer_engagement,
            us.geography,
            us.cal_date,
            us.measure,
            CASE
                WHEN us.measure = 'USAGE' THEN sum(us.units)
                ELSE 0
            END as ampv,
            CASE
                WHEN us.measure = 'HP_SHARE' THEN sum(us.units)
                ELSE 0
            END as share
        FROM prod.usage_share us
        where us.measure in ('USAGE', 'HP_SHARE')
            and us.version = (
                select max(version)
                from prod.usage_share
            )
        GROUP BY us.record,
            us.version,
            us.platform_subset,
            us.customer_engagement,
            us.geography,
            us.cal_date,
            us.measure
    ),
    usage_share2 as (
        SELECT distinct record,
            version,
            platform_subset,
            customer_engagement,
            geography,
            cal_date,
            sum(ampv) as ampv,
            sum(share) as share
        FROM usage_share1
        GROUP BY record,
            version,
            platform_subset,
            customer_engagement,
            geography,
            cal_date
    ),
    ib1 as (
        SELECT distinct ib.platform_subset,
            ib.customer_engagement,
            ib.cal_date,
            cc.market10,
            sum(ib.units) as totib
        FROM prod.ib ib
            LEFT JOIN mdm.iso_country_code_xref cc ON ib.country_alpha2 = cc.country_alpha2
        WHERE ib.version = (
                select max(version)
                from prod.ib
            )
            and ib.measure = 'IB'
        GROUP BY ib.platform_subset,
            ib.customer_engagement,
            ib.cal_date,
            cc.market10
    ),
    usage_share3 as (
        SELECT distinct us.record,
            us.version,
            us.platform_subset,
            us.customer_engagement,
            us.geography,
            us.cal_date,
            sum(us.ampv) as ampv,
            sum(us.share) as share,
            sum(ib.totib) as totib
        FROM usage_share2 us
            LEFT JOIN ib1 ib on us.platform_subset = ib.platform_subset
            and us.customer_engagement = ib.customer_engagement
            and us.cal_date = ib.cal_date
            and us.geography = ib.market10
        GROUP BY us.record,
            us.version,
            us.platform_subset,
            us.customer_engagement,
            us.geography,
            us.cal_date
    ),
    usaget as (
        select record,
            version,
            platform_subset,
            customer_engagement,
            geography,
            cal_date,
            ampv * totib as total_pages,
            ampv * share * totib as hp_pages --,totib as us_ib
        from usage_share3
    )
SELECT DISTINCT ib.record,
    ib.version,
    GETDATE() load_date,
    ib.cal_date,
    iso.region_5,
    iso.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    ib.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    ib.measure,
    SUM(ib.units) units -- should be latest offcial, Latest official published IB--
,
    ib.official
FROM prod.ib
    inner join dt_list dt on dt.Date = ib.cal_date
    left join mdm.hardware_xref hw on ib.platform_subset = hw.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = ib.customer_engagement
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdm.iso_country_code_xref iso on iso.country_alpha2 = ib.country_alpha2
    left join mdate on 1 = 1
    left join yr_range on 1 = 1
where ib.version = (
        select max(version)
        FROM prod.ib
        WHERE official = 1
    )
    and ib.measure = 'IB'
    and plx.pl_scenario = 'DRIVERS'
    and ib.cal_date <= mdate.max_date
    and iso.region_5 NOT IN ('XU', 'XW')
GROUP BY ib.record,
    ib.version,
    ib.cal_date,
    iso.region_5,
    iso.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    ib.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    ib.measure,
    ib.official
union all
SELECT DISTINCT norm.record,
    norm.version,
    GETDATE() load_date,
    norm.cal_date,
    iso.region_5,
    iso.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    norm.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'HW_UNITS' as measure --Latest verison of norm shipments. Printer shipments from EDW + LT hw forecast + Flash 
,
    SUM(norm.units) units,
    TRUE as official
FROM prod.norm_shipments_ce norm
    inner join dt_list dt on dt.Date = norm.cal_date
    left join mdm.hardware_xref hw on norm.platform_subset = hw.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = norm.customer_engagement
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdm.iso_country_code_xref iso on iso.country_alpha2 = norm.country_alpha2
    left join mdate on 1 = 1
where norm.version = (
        select max(version)
        FROM prod.norm_shipments_ce
    )
    and plx.pl_scenario = 'DRIVERS'
    and norm.cal_date <= mdate.max_date --and YEAR(norm.cal_date) IN (2019,2020,2021,2022)
    and iso.region_5 NOT IN ('XU', 'XW')
GROUP BY norm.record,
    norm.version,
    norm.cal_date,
    iso.region_5,
    iso.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    norm.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
union all
--update to the working forecast when ready
SELECT DISTINCT crg.record,
    crg.version,
    GETDATE() load_date,
    crg.cal_date,
    CASE
        WHEN crg.geography = 'CENTRAL EUROPE' THEN 'EU'
        WHEN crg.geography = 'NORTHERN EUROPE' THEN 'EU'
        WHEN crg.geography = 'SOUTHERN EUROPE' THEN 'EU'
        WHEN crg.geography = 'UK&I' THEN 'EU'
        WHEN crg.geography = 'LATIN AMERICA' THEN 'LA'
        WHEN crg.geography = 'INDIA SL & BL' THEN 'AP'
        WHEN crg.geography = 'GREATER ASIA' THEN 'AP'
        WHEN crg.geography = 'GREATER CHINA' THEN 'AP'
        WHEN crg.geography = 'ISE' THEN 'EU'
        WHEN crg.geography = 'NORTH AMERICA' THEN 'NA'
    END AS region_5,
    crg.geography,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    crg.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'CRGS_WO_HOST_PACKS' as measure ---check wiht Brent M and Mark only laser 
,
    SUM(crg.adjusted_cartridges) cartridges,
    TRUE as official
FROM prod.working_forecast crg
    inner join dt_list dt on dt.Date = crg.cal_date
    left join mdm.hardware_xref hw on crg.platform_subset = hw.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = crg.customer_engagement
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl --left join mdm.iso_country_code_xref iso on iso.market10=crg.geography
    left join mdate on 1 = 1
where crg.version = (
        select max(version)
        FROM prod.working_forecast
    )
    and plx.pl_scenario = 'DRIVERS'
    and crg.cal_date <= mdate.max_date --and iso.region_5 NOT IN ('XU','XW')
GROUP BY crg.record,
    crg.version,
    crg.cal_date --,iso.region_5
,
    crg.geography,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    crg.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT crg.record,
    crg.version,
    GETDATE() load_date,
    crg.cal_date,
    CASE
        WHEN crg.geography = 'CENTRAL EUROPE' THEN 'EU'
        WHEN crg.geography = 'NORTHERN EUROPE' THEN 'EU'
        WHEN crg.geography = 'SOUTHERN EUROPE' THEN 'EU'
        WHEN crg.geography = 'UK&I' THEN 'EU'
        WHEN crg.geography = 'LATIN AMERICA' THEN 'LA'
        WHEN crg.geography = 'INDIA SL & BL' THEN 'AP'
        WHEN crg.geography = 'GREATER ASIA' THEN 'AP'
        WHEN crg.geography = 'GREATER CHINA' THEN 'AP'
        WHEN crg.geography = 'ISE' THEN 'EU'
        WHEN crg.geography = 'NORTH AMERICA' THEN 'NA'
    END AS region_5,
    crg.geography,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    crg.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'CRGS_WO_HOST_SINGLES' as measure ---check wiht Brent M and Mark only laser 
,
    SUM(
        crg.adjusted_cartridges * s.equivalents_multiplier
    ) cartridges,
    TRUE as official
FROM prod.working_forecast crg
    inner join dt_list dt on dt.Date = crg.cal_date
    left join mdm.hardware_xref hw on crg.platform_subset = hw.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = crg.customer_engagement
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdm.supplies_xref s on s.base_product_number = crg.base_product_number
    left join mdate on 1 = 1
where crg.version = (
        select max(version)
        FROM prod.working_forecast
    )
    and plx.pl_scenario = 'DRIVERS'
    and crg.cal_date <= mdate.max_date --and iso.region_5 NOT IN ('XU','XW')
GROUP BY crg.record,
    crg.version,
    crg.cal_date --,iso.region_5
,
    crg.geography,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    crg.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT crg.record,
    crg.version,
    GETDATE() load_date,
    crg.cal_date,
    CASE
        WHEN crg.geography = 'CENTRAL EUROPE' THEN 'EU'
        WHEN crg.geography = 'NORTHERN EUROPE' THEN 'EU'
        WHEN crg.geography = 'SOUTHERN EUROPE' THEN 'EU'
        WHEN crg.geography = 'UK&I' THEN 'EU'
        WHEN crg.geography = 'LATIN AMERICA' THEN 'LA'
        WHEN crg.geography = 'INDIA SL & BL' THEN 'AP'
        WHEN crg.geography = 'GREATER ASIA' THEN 'AP'
        WHEN crg.geography = 'GREATER CHINA' THEN 'AP'
        WHEN crg.geography = 'ISE' THEN 'EU'
        WHEN crg.geography = 'NORTH AMERICA' THEN 'NA'
    END AS region_5,
    crg.geography,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    crg.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'CRGS' as measure ---check wiht Brent M and Mark only laser 
,
    SUM(crg.adjusted_cartridges) + SUM(crg.host_cartridges) cartridges,
    TRUE as official
FROM prod.working_forecast crg
    inner join dt_list dt on dt.Date = crg.cal_date
    left join mdm.hardware_xref hw on crg.platform_subset = hw.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = crg.customer_engagement
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl -- left join mdm.iso_country_code_xref iso on iso.market10=crg.geography
    left join mdate on 1 = 1
where crg.version = (
        select max(version)
        FROM prod.working_forecast
    )
    and plx.pl_scenario = 'DRIVERS'
    and crg.cal_date <= mdate.max_date -- and iso.region_5 NOT IN ('XU','XW')
GROUP BY crg.record,
    crg.version,
    crg.cal_date --  ,iso.region_5
,
    crg.geography,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    crg.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT fin.record_type record,
    fin.version,
    GETDATE() load_date,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'TOTAL_NET_REVENUE' measure --*laser only should be final forecast revenue. from John and Gretchentied to drivers. *check with Priyanka on  how this is gettting built today. 
,
    SUM(net_revenue) total_net_revenue,
    TRUE as official
FROM fin_prod.actuals_plus_forecast_financials fin
    inner join dt_list dt on dt.Date = fin.cal_date
    left join mdm.hardware_xref hw on hw.platform_subset = fin.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = fin.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE fin.version = (
        SELECT MAX(version)
        FROM fin_prod.actuals_plus_forecast_financials
    )
    and plx.pl_scenario = 'DRIVERS'
    and fin.cal_date <= mdate.max_date
GROUP BY fin.record_type,
    fin.version,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT fin.record_type record,
    fin.version,
    GETDATE() load_date,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'VTC_REVENUE' measure ----*laser onl;y (Shipment_Net_Revenue - Demand_Net_Revenue(consumption revenue))
,
    SUM(Shipment_Net_Revenue - Demand_Net_Revenue) Vtc_Revenue,
    TRUE as official
FROM fin_prod.actuals_plus_forecast_financials fin
    inner join dt_list dt on dt.Date = fin.cal_date
    left join mdm.hardware_xref hw on hw.platform_subset = fin.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = fin.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE fin.version = (
        SELECT MAX(version)
        FROM fin_prod.actuals_plus_forecast_financials
    )
    and plx.pl_scenario = 'DRIVERS'
    and fin.cal_date <= mdate.max_date
GROUP BY fin.record_type,
    fin.version,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT fin.record_type record,
    fin.version,
    GETDATE() load_date,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'CONSUMPTION_REVENUE' measure ---consumption rev - net rev based on consumption demand L
,
    SUM(Demand_Net_Revenue) Consumption_Revenue,
    TRUE as official
FROM fin_prod.actuals_plus_forecast_financials fin
    inner join dt_list dt on dt.Date = fin.cal_date
    left join mdm.hardware_xref hw on hw.platform_subset = fin.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = fin.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE fin.version = (
        SELECT MAX(version)
        FROM fin_prod.actuals_plus_forecast_financials
    )
    and plx.pl_scenario = 'DRIVERS'
    and fin.cal_date <= mdate.max_date --and YEAR(fin.cal_date) IN (2019,2020,2021,2022)
GROUP BY fin.record_type,
    fin.version,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT fin.record_type record,
    fin.version,
    GETDATE() load_date,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'REV_CC' measure ----Revenue per Page for Laser L
,
    SUM(
        Net_Revenue / NULLIF(yield_x_units_black_only, 0)
    ) rev_cc,
    TRUE as official
FROM fin_prod.actuals_plus_forecast_financials fin
    inner join dt_list dt on dt.Date = fin.cal_date
    left join mdm.hardware_xref hw on hw.platform_subset = fin.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = fin.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE fin.version = (
        SELECT MAX(version)
        FROM fin_prod.actuals_plus_forecast_financials
    )
    and plx.pl_scenario = 'DRIVERS'
    and fin.cal_date <= mdate.max_date --and plx.pl_level_1 = 'LASER' 
GROUP BY fin.record_type,
    fin.version,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT fin.record_type record,
    fin.version,
    GETDATE() load_date,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'REV_IB' measure -----Total Revenue per Total IB
,
    SUM(fin.Net_Revenue) / NULLIF(SUM(ib.units), 0) rev_ib ----L
,
    TRUE as official
FROM fin_prod.actuals_plus_forecast_financials fin
    inner join dt_list dt on dt.Date = fin.cal_date
    LEFT JOIN prod.ib ib on ib.platform_subset = fin.platform_subset
    and ib.cal_date = fin.cal_date
    and ib.country_alpha2 = fin.country_alpha2
    and ib.customer_engagement = fin.customer_engagement
    and ib.version = (
        select max(version)
        FROM prod.ib
    )
    and ib.measure = 'IB'
    left join mdm.iso_country_code_xref iso on iso.country_alpha2 = fin.country_alpha2
    left join mdm.hardware_xref hw on hw.platform_subset = fin.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = fin.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE fin.version = (
        SELECT MAX(version)
        FROM fin_prod.actuals_plus_forecast_financials
    )
    and plx.pl_scenario = 'DRIVERS'
    and fin.cal_date <= mdate.max_date --and YEAR(fin.cal_date) IN (2019,2020,2021,2022) 
    and iso.region_5 NOT IN ('XU', 'XW')
GROUP BY fin.record_type,
    fin.version,
    fin.cal_date,
    fin.region_5,
    fin.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    fin.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT us.record,
    us.version,
    GETDATE() load_date,
    us.cal_date,
    iso.region_5,
    us.geography,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    us.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'TOTAL_PAGES' measure --------L only 
,
    SUM(us.total_pages) units,
    TRUE as official
FROM usaget us
    inner join dt_list dt on dt.Date = us.cal_date
    left join (
        select distinct region_5,
            market10
        from mdm.iso_country_code_xref
    ) iso on iso.market10 = us.geography
    left join mdm.hardware_xref hw on hw.platform_subset = us.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = us.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE 1 = 1
    AND plx.pl_scenario = 'DRIVERS'
    and us.cal_date <= mdate.max_date --and YEAR(us.cal_date) IN (2019,2020,2021,2022)
    and iso.region_5 NOT IN ('XU', 'XW')
GROUP BY us.record,
    us.version,
    us.cal_date,
    iso.region_5,
    us.geography,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    us.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT us.record,
    us.version,
    GETDATE() load_date,
    us.cal_date,
    iso.region_5,
    us.geography,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    us.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    'HP_PAGES' measure ----L only 
,
    SUM(hp_pages) units,
    TRUE as official
FROM usaget us
    inner join dt_list dt on dt.Date = us.cal_date
    left join (
        select distinct region_5,
            market10
        from mdm.iso_country_code_xref
    ) iso on iso.market10 = us.geography
    left join mdm.hardware_xref hw on hw.platform_subset = us.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    left join biz_segments b on b.hw_product_family = hw.hw_product_family
    and b.customer_engagement = us.customer_engagement
    inner join tech t on t.pl = hw.pl
    left join mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE 1 = 1
    AND plx.pl_scenario = 'DRIVERS'
    and us.cal_date <= mdate.max_date --and YEAR(us.cal_date) IN (2019,2020,2021,2022)
    and iso.region_5 NOT IN ('XU', 'XW')
GROUP BY us.record,
    us.version,
    us.cal_date,
    iso.region_5,
    us.geography,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    us.customer_engagement,
    b.Business_Segment1,
    b.Business_Segment2,
    b.Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3
UNION
SELECT DISTINCT '4-BOX' record,
    date_part('year', getdate()-1) || '.' || date_part('month', getdate()-1) || '.' || date_part('day', getdate()-1) || '.1' AS version,
    GETDATE() load_date,
    b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    '' customer_engagement,
    '' Business_Segment1,
    '' Business_Segment2,
    '' Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure, -------------L only Consumptiopn HP Pages ('k_hp_demand') black only HP pages based on demand 
    SUM(units) units,
    TRUE as official
FROM prod.fourbox b4
    inner join dt_list dt on dt.Date = b4.cal_date
    LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = b4.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family --left join biz_segments b on b.hw_product_family = hw.hw_product_family --and b.customer_engagement = b4.customer_engagement
    inner join tech t on t.pl = hw.pl
    LEFT JOIN mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE measure = 'CONS_HP_CCPG'
    and plx.pl_scenario = 'DRIVERS'
    and b4.cal_date <= mdate.max_date --and YEAR(b4.cal_date) IN (2019,2020,2021,2022)
GROUP BY b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure
UNION
SELECT DISTINCT '4-BOX' record,
    date_part('year', getdate()-1) || '.' || date_part('month', getdate()-1) || '.' || date_part('day', getdate()-1) || '.1' AS version,
    GETDATE() load_date,
    b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    '' customer_engagement,
    '' Business_Segment1,
    '' Business_Segment2,
    '' Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure,
    SUM(units) units, --L only Reported HP Pages  black only HP pages based on yield 
    TRUE as official
FROM prod.fourbox b4
    inner join dt_list dt on dt.Date = b4.cal_date
    LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = b4.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    inner join tech t on t.pl = hw.pl
    LEFT JOIN mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE measure = 'REP_HP_CCPG'
    and plx.pl_scenario = 'DRIVERS'
    and b4.cal_date <= mdate.max_date --and YEAR(b4.cal_date) IN (2019,2020,2021,2022)
GROUP BY b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure
UNION
SELECT DISTINCT '4-BOX' record,
    date_part('year', getdate()-1) || '.' || date_part('month', getdate()-1) || '.' || date_part('day', getdate()-1) || '.1' AS version,
    GETDATE() load_date,
    b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    '' customer_engagement,
    '' Business_Segment1,
    '' Business_Segment2,
    '' Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure, ------Reported HP pages / HP share 
    SUM(units) units,
    TRUE as official
FROM prod.fourbox b4
    inner join dt_list dt on dt.Date = b4.cal_date
    LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = b4.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    inner join tech t on t.pl = hw.pl
    LEFT JOIN mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE measure = 'REP_DEV_CCPG'
    and plx.pl_scenario = 'DRIVERS'
    and b4.cal_date <= mdate.max_date --and YEAR(b4.cal_date) IN (2019,2020,2021,2022)
GROUP BY b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure
UNION
SELECT DISTINCT '4-BOX' record,
    date_part('year', getdate()-1) || '.' || date_part('month', getdate()-1) || '.' || date_part('day', getdate()-1) || '.1' AS version,
    GETDATE() load_date,
    b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    CASE
        WHEN hw.technology = 'LASER' THEN 'N/A'
        ELSE ISNULL(m.m_nm, 'NON-MATURE')
    END AS m_nm,
    '' customer_engagement,
    '' Business_Segment1,
    '' Business_Segment2,
    '' Business_Segment3,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure, ----L only ('k_hp_demand','k_non_hp_demand','k_host_demand','color_host_demand','color_non_hp_demand','color_hp_demand') black only HP pages based on demand 
    SUM(units) units,
    TRUE as official
FROM prod.fourbox b4
    inner join dt_list dt on dt.Date = b4.cal_date
    LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = b4.platform_subset
    left join matures m ON m.hw_product_family = hw.hw_product_family
    inner join tech t on t.pl = hw.pl
    LEFT JOIN mdm.product_line_scenarios_xref plx on hw.pl = plx.pl
    left join mdate on 1 = 1
WHERE measure = 'CONS_DEVICE_CCPG'
    and plx.pl_scenario = 'DRIVERS'
    and b4.cal_date <= mdate.max_date --and YEAR(b4.cal_date) IN (2019,2020,2021,2022)
GROUP BY b4.cal_date,
    b4.region_5,
    b4.market10,
    hw.technology,
    hw.hw_product_family,
    m.m_nm,
    hw.supplies_mkt_cat,
    hw.business_feature,
    hw.brand,
    plx.pl_level_1,
    plx.pl_level_2,
    plx.pl_level_3,
    measure;