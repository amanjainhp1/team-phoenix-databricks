# Databricks notebook source
# MAGIC %md
# MAGIC # Installed Base Promotion

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

norm_ships = """


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , sub_version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('ib', 'norm_shipments')
    AND version = '2022.03.29.1'
)SELECT ns.record
    , ns.cal_date
    , ns.region_5
    , ns.country_alpha2
    , ns.platform_subset
    , ns.units
    , vars.version
    , vars.load_date
FROM "stage"."norm_ships" AS ns
CROSS JOIN ib_promo_01_filter_vars AS vars
WHERE vars.record = 'norm_shipments'
"""

query_list.append(["prod.norm_shipments", norm_ships])

# COMMAND ----------

ib_source = """


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , sub_version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('ib', 'norm_shipments')
    AND version = '2022.03.29.1'
)SELECT ib.record
    , vars.version
    , vars.load_date
    , ib.month_begin AS cal_date
    , ib.country_alpha2 AS country
    , ib.platform_subset
    , UPPER(ib.split_name) AS customer_engagement
    , 'ib' AS measure
    , ib.ib AS units
FROM "stage"."ib_staging" AS ib
JOIN ib_promo_01_filter_vars AS vars
    ON vars.record = ib.record
WHERE 1=1
    AND ib.record = 'ib'
"""

query_list.append(["prod.ib_source", ib_source])

# COMMAND ----------

ib = """


SELECT record
    , version
    , load_date
    , cal_date
    , country
    , platform_subset
    , customer_engagement
    , measure
    , units
    , 0 AS official
FROM "prod"."ib_source"
"""

query_list.append(["prod.ib", ib])

# COMMAND ----------

ib_datamart_source = """


with ib_promo_05_rdma_pl as (


SELECT DISTINCT rdma.platform_subset
    , rdma.PL
    , plx.technology
    , pls.PL_level_1
FROM "mdm"."rdma" AS rdma WITH (NOLOCK)
INNER JOIN "mdm"."product_line_xref" AS plx
    ON plx.pl = rdma.PL
LEFT JOIN "mdm"."product_line_scenarios_xref" AS pls
    ON pls.PL = rdma.PL
WHERE 1=1
    AND pls.pl_scenario = 'IB-dashboard'
),  ib_promo_06_rdma as (


-- waiting on an upstream fix; this removes dupes for these pfs
SELECT DISTINCT rdma.platform_subset
    , rdma.PL
    , rdma.Product_Family
    , plx.pl_level_1
FROM "mdm"."rdma" AS rdma WITH (NOLOCK)
LEFT JOIN ib_promo_05_rdma_pl AS plx
    ON plx.PL = rdma.PL
where 1=1
    and rdma.Platform_Subset not in ('CRICKET','EVERETT','FORRESTER','MAYBACH')

union all

SELECT DISTINCT rdma.platform_subset
    , rdma.PL
    , rdma.Product_Family
    , plx.pl_level_1
FROM "mdm"."rdma" AS rdma WITH (NOLOCK)
LEFT JOIN ib_promo_05_rdma_pl AS plx
    ON plx.PL = rdma.PL
where 1=1
    and ((rdma.Platform_Subset = 'CRICKET' and rdma.pl = '2Q')
        or (rdma.Platform_Subset = 'EVERETT' and rdma.pl = '3Y')
        or (rdma.Platform_Subset = 'FORRESTER' and rdma.pl = '3Y')
        or (rdma.Platform_Subset = 'MAYBACH' and rdma.pl = '3Y'))
),  ib_promo_07_hw_fcst as (


SELECT MAX(cal_date) AS max_date
FROM "prod"."hardware_ltf"
WHERE 1=1
    AND record = 'hw_fcst'
    AND official = 1
)SELECT 'IB' AS record
    , ib.cal_date
    , iso.region_5
    , ib.country AS iso_alpha2
    , iso.country AS country_name
    , cc.country_level_2 AS market10
    , ib.platform_subset
    , COALESCE(rdmapl.PL, hw.pl) AS pl
    , rdmapl.product_family
    , COALESCE(rdmapl.pl_level_1, pls.pl_level_1) AS business_category
    , ib.customer_engagement
    , hw.business_feature AS hw_hps_ops
    , hw.technology AS technology
    , hw.hw_product_family AS tech_split
    , hw.brand
    , SUM(CASE WHEN ib.measure = 'ib' THEN ib.units END) AS IB
    , NULL AS printer_installs
    , ib.version
    , CAST(ib.load_date AS DATE) AS load_date
    , CAST(ib.cal_date AS VARCHAR(25)) + '-' + ib.platform_subset + '-' + ib.country + '-' + ib.customer_engagement AS composite_key
FROM "stage"."ib_dbt_source" AS ib
LEFT JOIN ib_promo_06_rdma AS rdmapl
    ON rdmapl.platform_subset = ib.platform_subset
LEFT JOIN "prod"."iso_country_code_xref" AS iso
    ON iso.country_alpha2 = ib.country
LEFT JOIN "mdm"."iso_cc_rollup_xref" AS cc
    ON cc.country_alpha2 = ib.country
LEFT JOIN "mdm"."hardware_xref" AS hw
    ON hw.platform_subset = ib.platform_subset
LEFT JOIN "mdm"."product_line_scenarios_xref" AS pls
    ON pls.pl = COALESCE(rdmapl.PL, hw.pl)
    AND pls.pl_scenario = 'IB-dashboard'
LEFT JOIN ib_promo_07_hw_fcst AS max_f
    ON 1=1
WHERE 1=1
    AND cc.country_scenario = 'Market10'
    AND ib.cal_date <= max_f.max_date
    AND hw.technology IN ('LASER','INK','PWA')
GROUP BY ib.cal_date
    , iso.region_5
    , ib.country
    , iso.country
    , cc.country_level_2
    , ib.platform_subset
    , rdmapl.PL
    , hw.pl
    , rdmapl.product_family
    , rdmapl.pl_level_1
    , pls.pl_level_1
    , ib.customer_engagement
    , hw.business_feature
    , hw.technology
    , hw.hw_product_family
    , hw.brand
    , ib.version
    , ib.load_date
"""

query_list.append(["prod.ib_datamart_source", ib_datamart_source])

# COMMAND ----------

norm_ships_split_lag = """


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , sub_version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('ib', 'norm_shipments')
    AND version = '2022.03.29.1'
)SELECT ib.record
    , vars.version
    , vars.load_date
    , ib.month_begin AS cal_date
    , ib.country_alpha2 AS country
    , ib.platform_subset
    , UPPER(ib.split_name) AS customer_engagement
    , 'printer_installs' AS measure
    , ib.printer_installs AS units
FROM "stage"."ib_staging" AS ib
JOIN ib_promo_01_filter_vars AS vars
    ON vars.record = ib.record
WHERE 1=1
    AND ib.record = 'ib'
    AND ib.printer_installs <> 0
    AND NOT ib.printer_installs IS NULL
"""

query_list.append(["prod.norm_ships_split_lag", norm_ships_split_lag])

# COMMAND ----------

norm_shipments_ce = """


with ib_promo_01_filter_vars as (


SELECT record
    , version
    , sub_version
    , source_name
    , load_date
    , official
FROM "prod"."version"
WHERE record in ('ib', 'norm_shipments')
    AND version = '2022.03.29.1'
)SELECT 'norm_ships_ce' AS record
    , vars.version
    , vars.load_date
    , ns.month_begin AS cal_date
    , ns.region_5 AS region_5
    , ns.country_alpha2
    , ns.platform_subset
    , UPPER(ns.split_name) AS customer_engagement
    , ns.split_value
    , ns.units AS units
FROM "stage"."ib_04_units_ce_splits_pre" AS ns
CROSS JOIN ib_promo_01_filter_vars AS vars
WHERE 1=1
    AND vars.record = 'norm_shipments'
"""

query_list.append(["prod.norm_shipments_ce", norm_shipments_ce])

#TODO: Add a mode parameter to query_list

# COMMAND ----------

# MAGIC %run "../../common/output_to_redshift" $query_list=query_list
