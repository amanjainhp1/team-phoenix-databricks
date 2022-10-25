# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

ib_datamart_source_code = """

create or replace view prod.ib_datamart_source_vw as
with dbt__CTE__ib_promo_05_rdma_pl as (
    SELECT DISTINCT rdma.platform_subset
        , rdma.PL
        , plx.technology
        , pls.PL_level_1
    FROM mdm.rdma AS rdma
    INNER JOIN mdm.product_line_xref AS plx
        ON plx.pl = rdma.PL
    LEFT JOIN mdm.product_line_scenarios_xref AS pls
        ON pls.PL = rdma.PL
    WHERE 1=1
        AND pls.pl_scenario = 'IB-DASHBOARD'
),  

dbt__CTE__ib_promo_06_rdma as (
    SELECT DISTINCT rdma.platform_subset
        , rdma.PL
        , rdma.Product_Family
        , plx.pl_level_1
    FROM mdm.rdma AS rdma
    LEFT JOIN dbt__CTE__ib_promo_05_rdma_pl AS plx
        ON plx.PL = rdma.PL
    where 1=1
        and rdma.Platform_Subset not in ('CRICKET','EVERETT','FORRESTER','MAYBACH')

    union all

    SELECT DISTINCT rdma.platform_subset
        , rdma.PL
        , rdma.Product_Family
        , plx.pl_level_1
    FROM mdm.rdma AS rdma
    LEFT JOIN dbt__CTE__ib_promo_05_rdma_pl AS plx
        ON plx.PL = rdma.PL
    where 1=1
        and ((rdma.Platform_Subset = 'CRICKET' and rdma.pl = '2Q')
            or (rdma.Platform_Subset = 'EVERETT' and rdma.pl = '3Y')
            or (rdma.Platform_Subset = 'FORRESTER' and rdma.pl = '3Y')
            or (rdma.Platform_Subset = 'MAYBACH' and rdma.pl = '3Y'))
),

dbt__CTE__ib_promo_07_hw_fcst as (
    SELECT MAX(cal_date) AS max_date
    FROM prod.hardware_ltf
    WHERE 1=1
        AND record = 'HW_FCST'
        AND official = 1
)

SELECT 
	'IB' AS record
    , ib.cal_date
    , iso.region_5
    , ib.country_alpha2  AS iso_alpha2
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
    , SUM(CASE WHEN ib.measure = 'IB' THEN ib.units END) AS IB
    , NULL AS printer_installs
    , ib.version
    , CAST(ib.load_date AS DATE) AS load_date
    , CAST(ib.cal_date AS VARCHAR(25)) + '-' + ib.platform_subset + '-' + ib.country_alpha2  + '-' + ib.customer_engagement AS composite_key
FROM prod.ib AS ib
LEFT JOIN dbt__CTE__ib_promo_06_rdma AS rdmapl
    ON rdmapl.platform_subset = ib.platform_subset
LEFT JOIN mdm.iso_country_code_xref AS iso
    ON iso.country_alpha2 = ib.country_alpha2 
LEFT JOIN mdm.iso_cc_rollup_xref AS cc
    ON cc.country_alpha2 = ib.country_alpha2 
LEFT JOIN mdm.hardware_xref AS hw
    ON hw.platform_subset = ib.platform_subset
LEFT JOIN mdm.product_line_scenarios_xref AS pls
    ON pls.pl = COALESCE(rdmapl.PL, hw.pl)
    AND pls.pl_scenario = 'IB-DASHBOARD'
LEFT JOIN dbt__CTE__ib_promo_07_hw_fcst AS max_f
    ON 1=1
WHERE 1=1
    and ib.version = (select max(version) from prod.ib)
    AND cc.country_scenario = 'MARKET10'
    AND ib.cal_date <= max_f.max_date
    AND hw.technology IN ('LASER','INK','PWA')
GROUP BY ib.cal_date
    , iso.region_5
    , ib.country_alpha2 
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
    , ib.load_date;
    
GRANT ALL ON prod.ib_datamart_source_vw to GROUP phoenix_dev;
"""

# COMMAND ----------

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
    .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
con = psycopg2.connect(conn_string)
cur = con.cursor()

cur.execute(ib_datamart_source_code)
con.commit()

cur.close()


# COMMAND ----------

drop_view_code = """

drop view prod.ib_datamart_source_vw
"""

# COMMAND ----------

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'" \
    .format(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"])
con = psycopg2.connect(conn_string)
cur = con.cursor()

cur.execute(drop_view_code)
con.commit()

cur.close()
