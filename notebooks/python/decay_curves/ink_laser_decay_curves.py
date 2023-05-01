# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ink_laser_decay_curves

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ink decay curves

# COMMAND ----------

ink_decay_curve = """WITH geo as (
SELECT DISTINCT country_level_3 AS region_5
    , country_level_2 AS region_6
    , iso.market10
    , concat(iso.market10,concat('-', CASE WHEN iso.developed_emerging = 'Developed' THEN 'DM' ELSE 'EM' END)) AS market13
    , CASE WHEN iso.developed_emerging = 'Developed' THEN 'DM' ELSE 'EM' END AS EM_DM  
FROM mdm.iso_cc_rollup_xref cc 
LEFT JOIN mdm.iso_country_code_xref iso
    ON iso.country_alpha2 = cc.country_alpha2 
WHERE country_scenario = 'Region_6' 
AND country_level_2 IS NOT NULL 
AND country_level_3 <> 'JP' 
AND market13 NOT IN ('UK&I-EM', 'NORTHERN EUROPE-EM', 'SOUTHERN EUROPE-EM' )),

norm as (
  ---Norm Shipments CE and updated brand info -----
SELECT DISTINCT norm.platform_subset
    , CASE WHEN hw.technology = 'INK' AND hw.platform_subset LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-IA' 
           WHEN hw.technology = 'INK' AND hw.platform_subset NOT LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-NON-IA' 
           WHEN hw.brand = 'PLE H' THEN 'PLE-H' 
           WHEN hw.brand = 'PLE L' THEN 'PLE-L' 
      ELSE hw.brand 
      END
      AS ink_brand_adjust 
    , hw.product_lifecycle_status AS product_lifecycle_status 
    , hw.predecessor 
    , hw.technology 
    , norm.customer_engagement 
    , concat(iso.market10,concat('-', CASE WHEN iso.developed_emerging = 'Developed' THEN 'DM' ELSE 'EM' END)) AS market13  
FROM prod.norm_shipments_ce norm
INNER JOIN mdm.hardware_xref hw
    ON hw.platform_subset = norm.platform_subset 
INNER JOIN mdm.iso_country_code_xref iso
    ON iso.country_alpha2 = norm.country_alpha2 
WHERE norm.version = (SELECT MAX(version) FROM prod.norm_shipments_ce) 
AND hw.technology IN ('INK')
-- and ib.official = 1
-- Pull NPI's from latest Norm Ships CE
),
npi as (
SELECT DISTINCT platform_subset
    , ink_brand_adjust
    , product_lifecycle_status
    , predecessor
    , technology
    , customer_engagement
    , market13  
FROM norm
WHERE product_lifecycle_status = 'N' 
),
decay_curves as (
SELECT * FROM
    (SELECT DISTINCT CASE WHEN hw.technology = 'INK' AND hw.platform_subset LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-IA' 
                  WHEN hw.technology = 'INK' AND hw.platform_subset NOT LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-NON-IA' 
                  WHEN hw.brand = 'PLE H' THEN 'PLE-H' 
                  WHEN hw.brand = 'PLE L' THEN 'PLE-L' 
             ELSE hw.brand 
             END AS brand
          , split_name
          , geography
          , YEAR
          , decay_m13.platform_subset
          , hw.product_lifecycle_status
          , ISNULL(round(VALUE, 2), 0) AS val 
      FROM prod.decay_m13 decay_m13
      LEFT JOIN mdm.hardware_xref hw
      ON hw.platform_subset = decay_m13.platform_subset 
      WHERE hw.technology IN ('PWA', 'Ink')
       ) AS p
      PIVOT ( Sum(val)
      FOR year IN ('YEAR_1',
                  'YEAR_2',
                  'YEAR_3',
                  'YEAR_4',
                  'YEAR_5',
                  'YEAR_6',
                  'YEAR_7',
                  'YEAR_8',
                  'YEAR_9',
                  'YEAR_10',
                  'YEAR_11',
                  'YEAR_12',
                  'YEAR_13',
                  'YEAR_14',
                  'YEAR_15',
                  'YEAR_16',
                  'YEAR_17',
                  'YEAR_18',
                  'YEAR_19',
                  'YEAR_20',
                  'YEAR_21',
                  'YEAR_22',
                  'YEAR_23',
                  'YEAR_24',
                  'YEAR_25',
                  'YEAR_26',
                  'YEAR_27',
                  'YEAR_28',
                  'YEAR_29',
                  'YEAR_30') ) AS pivottable),

decays as ( 
SELECT brand
    , split_name
    , geography
    , platform_subset
    , product_lifecycle_status
    , ISNULL(year_1, 0) AS year_1
    , ISNULL(year_2, 0) AS year_2
    , ISNULL(year_3, 0) AS year_3
    , ISNULL(year_4, 0) AS year_4
    , ISNULL(year_5, 0) AS year_5
    , ISNULL(year_6, 0) AS year_6
    , ISNULL(year_7, 0) AS year_7
    , ISNULL(year_8, 0) AS year_8
    , ISNULL(year_9, 0) AS year_9
    , ISNULL(year_10, 0) AS year_10
    , ISNULL(year_11, 0) AS year_11
    , ISNULL(year_12, 0) AS year_12
    , ISNULL(year_13, 0) AS year_13
    , ISNULL(year_14, 0) AS year_14
    , ISNULL(year_15, 0) AS year_15
    , ISNULL(year_16, 0) AS year_16
    , ISNULL(year_17, 0) AS year_17
    , ISNULL(year_18, 0) AS year_18
    , ISNULL(year_19, 0) AS year_19
    , ISNULL(year_20, 0) AS year_20
    , ISNULL(year_21, 0) AS year_21
    , ISNULL(year_22, 0) AS year_22
    , ISNULL(year_23, 0) AS year_23
    , ISNULL(year_24, 0) AS year_24
    , ISNULL(year_25, 0) AS year_25
    , ISNULL(year_26, 0) AS year_26
    , ISNULL(year_27, 0) AS year_27
    , ISNULL(year_28, 0) AS year_28
    , ISNULL(year_29, 0) AS year_29
    , ISNULL(year_30, 0) AS year_30  
FROM decay_curves ),

decay_profiles as (
SELECT DISTINCT brand
    , split_name
    , geography
    , product_lifecycle_status
    , year_1
    , year_2
    , year_3
    , year_4
    , year_5
    , year_6
    , year_7
    , year_8
    , year_9
    , year_10
    , year_11
    , year_12
    , year_13
    , year_14
    , year_15
    , year_16
    , year_17
    , year_18
    , year_19
    , year_20
    , year_21
    , year_22
    , year_23
    , year_24
    , year_25
    , year_26
    , year_27
    , year_28
    , year_29
    , year_30 
    , COUNT(platform_subset) AS platform_subset_count
FROM decays 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34),
max_ps AS (
SELECT brand
    , split_name
    , geography
    , product_lifecycle_status
    , MAX(platform_subset_count) AS max_ps_count 
FROM decay_profiles 
GROUP BY 1,2,3,4 
),
ink_decays_npis as (
SELECT DISTINCT d.brand
    , d.split_name
    , d.geography
    , d.product_lifecycle_status
    , m.max_ps_count
    , year_1
    , year_2
    , year_3
    , year_4
    , year_5
    , year_6
    , year_7
    , year_8
    , year_9
    , year_10
    , year_11
    , year_12
    , year_13
    , year_14
    , year_15
    , year_16
    , year_17
    , year_18
    , year_19
    , year_20
    , year_21
    , year_22
    , year_23
    , year_24
    , year_25
    , year_26
    , year_27
    , year_28
    , year_29
    , year_30 
FROM decay_profiles d 
INNER JOIN max_ps m 
    ON m.brand = d.brand 
    AND m.geography = d.geography 
    AND m.split_name = d.split_name 
    AND m.max_ps_count = d.platform_subset_count 
    AND m.product_lifecycle_status = d.product_lifecycle_status 
WHERE d.product_lifecycle_status = 'N' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35),
npi_decays as (
SELECT concat('current ', concat(technology, ' decays'))::text AS record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , '' AS market10
    , '' AS EM_DM
    , market13
    , SUBSTRING(years, 6, 2) AS YEAR
    , customer_engagement AS split_name
    , VALUE 
FROM(SELECT n.platform_subset
         , n.ink_brand_adjust AS brand
         , n.product_lifecycle_status
         , n.predecessor
         , n.technology
         , n.customer_engagement
         , n.market13
         , year_1
         , year_2
         , year_3
         , year_4
         , year_5
         , year_6
         , year_7
         , year_8
         , year_9
         , year_10
         , year_11
         , year_12
         , year_13
         , year_14
         , year_15
         , year_16
         , year_17
         , year_18
         , year_19
         , year_20
         , year_21
         , year_22
         , year_23
         , year_24
         , year_25
         , year_26
         , year_27
         , year_28
         , year_29
         , year_30 
    FROM npi n 
    LEFT JOIN ink_decays_npis i 
    ON i.brand = n.ink_brand_adjust 
    AND i.split_name = n.customer_engagement 
    AND i.geography = n.market13 ) 
   UNPIVOT (value
   FOR years IN (year_1
                ,year_2
                ,year_3
                ,year_4
                ,year_5
                ,year_6
                ,year_7
                ,year_8
                ,year_9
                ,year_10
                ,year_11
                ,year_12
                ,year_13
                ,year_14
                ,year_15
                ,year_16
                ,year_17
                ,year_18
                ,year_19
                ,year_20
                ,year_21
                ,year_22
                ,year_23
                ,year_24
                ,year_25
                ,year_26
                ,year_27
                ,year_28
                ,year_29
                ,year_30)) as pivottable ), 
---- New Ink decay curves ----
decay as (

SELECT concat('current ', concat(de.technology, ' decays'))::text AS record
    , de.technology
    , de.product
    , norm.product_lifecycle_status
    , norm.platform_subset
    , geo.market10 AS market10
    , geo.EM_DM AS EM_DM
    , geo.market13
    , YEAR
    , split_name
    , VALUE 
FROM mdm.aul_upload_rev de
LEFT JOIN geo geo ON geo.region_6 = de.geography 
LEFT JOIN norm norm ON de.product = norm.ink_brand_adjust 
    AND norm.customer_engagement = de.split_name 
WHERE de.technology = 'Ink' 
    AND insert_ts = (SELECT MAX(insert_ts) FROM mdm.aul_upload_rev WHERE technology = 'INK')),
---- Current Decays for INK & PWA
c_decay_non_japan as (
SELECT concat('current ', concat(hw.technology, ' decays'))::text  AS record
    , hw.technology
    , CASE WHEN hw.technology = 'INK' AND hw.platform_subset LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-IA' 
                  WHEN hw.technology = 'INK' AND hw.platform_subset NOT LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-NON-IA' 
                  WHEN hw.brand = 'PLE H' THEN 'PLE-H' 
                  WHEN hw.brand = 'PLE L' THEN 'PLE-L' 
             ELSE hw.brand END AS brand
    , hw.product_lifecycle_status 
    , de.platform_subset
    , geo.market10 AS market10 
    , geo.EM_DM AS EM_DM 
    , geo.market13 
    , 'NON-JAPAN'::text AS area 
    , SUBSTRING(YEAR, 6, 2) AS YEAR 
    , split_name
    , VALUE
FROM prod.decay de 
LEFT JOIN geo geo 
ON geo.region_5 = de.geography 
LEFT JOIN mdm.hardware_xref hw 
ON hw.platform_subset = de.platform_subset 
WHERE de.official = 1  
    AND geography <> 'JP' 
    AND hw.technology IN ('ink', 'PWA')),
 -- Current decays for Japan

c_decay_japan as (
       
SELECT concat('current ', concat(hw.technology, ' decays'))::text  AS record
    , hw.technology
    , CASE WHEN hw.technology = 'INK' AND hw.platform_subset LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-IA' 
          WHEN hw.technology = 'INK' AND hw.platform_subset NOT LIKE '%IA' AND hw.brand = 'DJ' THEN 'DJ-NON-IA' 
          WHEN hw.brand = 'PLE H' THEN 'PLE-H' 
          WHEN hw.brand = 'PLE L' THEN 'PLE-L' 
     ELSE hw.brand END AS brand 
    , hw.product_lifecycle_status 
    , de.platform_subset
    , 'GREATER ASIA'::text AS market10 
    , 'DM'::text AS EM_DM 
    , 'GREATER ASIA-DM'::text AS market13 
    , 'JAPAN'::text AS area 
    , SUBSTRING(YEAR, 6, 2) AS YEAR 
    , split_name
    , VALUE
FROM prod.decay de 
LEFT JOIN geo geo 
    ON geo.region_5 = de.geography 
LEFT JOIN mdm.hardware_xref hw 
    ON hw.platform_subset = de.platform_subset 
WHERE de.official = 1  AND geography = 'JP' AND hw.technology IN ('ink', 'PWA')),

GADM as (
SELECT DISTINCT
    platform_subset,
    area 
FROM c_decay_non_japan 
WHERE market13 IN ('GREATER ASIA-DM')),

c_decay as (
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , j.platform_subset
    , market10
    , EM_DM
    , market13
    , 'JAPAN'::text as area
    , year::int
    , split_name
    , value
FROM c_decay_japan j
LEFT JOIN GADM GADM on GADM.platform_subset=j.platform_subset
WHERE j.market13 = 'Greater Asia-DM' and GADM.area is null
UNION ALL 
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , market10
    , EM_DM
    , market13
    , area
    , YEAR::int 
    , split_name
    , value
FROM c_decay_non_japan
),
union_step as (
SELECT  record
    , technology 
    , brand
    , product_lifecycle_status 
    , platform_subset
    , market10
    , EM_DM
    , market13
    , year::int
    , split_name
    , value
FROM c_decay
where technology = 'PWA'  
UNION ALL
------ PULL MATURES so there is no change to Mature Decay Curves -------
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , market10
    , EM_DM
    , market13
    , year::int
    , split_name
    , VALUE 
FROM c_decay
WHERE c_decay.technology = 'ink' 
    AND product_lifecycle_status = 'M' 
    AND brand <> 'PLE-L' 
UNION ALL
-------- PULL BIG INK so there is no change to Big Ink Decays -------
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , market10
    , EM_DM
    , market13
    , year::int
    , split_name
    , VALUE 
FROM c_decay
WHERE technology = 'ink' 
    AND brand = 'Big Ink' 
    AND product_lifecycle_status <> 'M' 
    AND product_lifecycle_status <> 'N' 
UNION ALL
------ Pull current EM curves that are not PWA, Big Ink, or Mature -----
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , market10
    , EM_DM
    , market13
    , year::int
    , split_name
    , VALUE 
FROM c_decay
WHERE technology = 'ink' 
    AND brand <> 'Big Ink' 
    AND product_lifecycle_status <> 'M' 
    AND em_dm = 'EM' 
    AND brand <> 'PLE-L' 
    AND product_lifecycle_status <> 'N' 
UNION ALL
------ Pull curves for PLE L -----
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , market10
    , EM_DM
    , market13
    , year::int
    , split_name
    , VALUE 
FROM c_decay
WHERE technology = 'ink' 
    AND brand = 'PLE-L' 
    AND product_lifecycle_status <> 'N' 
UNION ALL
-------- PULL NEW AULs in DM markets that are not mature, Big Ink, or PWA & are not I-Ink-------
SELECT record
  , technology
  , product as brand
  , product_lifecycle_status
  , platform_subset
  , market10
  , EM_DM
  , market13
  , year::int
  , split_name
  , VALUE 
FROM decay 
WHERE technology = 'ink' 
    AND product <> 'Big Ink' 
    AND product_lifecycle_status <> 'M' 
    AND em_dm = 'DM' 
    AND decay.product <> 'PLE-L' 
    AND product_lifecycle_status <> 'N' 
UNION ALL
------ PULL AUL's for NPI's ---------
SELECT record
    , technology
    , brand
    , product_lifecycle_status
    , platform_subset
    , g.market10
    , g.EM_DM
    , g.market13
    , year::int
    , split_name
    , VALUE 
FROM npi_decays np
INNER JOIN geo g 
ON g.market13 = np.market13 
)

SELECT record 
    , UPPER(technology) as technology
    , UPPER(brand) as brand
    , UPPER(product_lifecycle_status) as product_lifecycle_status
    , UPPER(platform_subset) as platform_subset
    , UPPER(market10) as market10
    , UPPER(EM_DM) as EM_DM
    , UPPER(market13) as market13
    , year
    , UPPER(split_name) as split_name
    , value
    , SYSDATE AS load_date
FROM union_step"""

# COMMAND ----------

ink_decay_df = read_redshift_to_df(configs) \
  .option("query",ink_decay_curve) \
  .load()
print("Loaded ink decay curves data in df")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Laser decay curve

# COMMAND ----------

laser_decay_curve = """WITH geo as (
SELECT 
    DISTINCT country_level_3 AS region_5
    , country_level_2 AS region_6
    , iso.market10
    , concat(iso.market10,concat('-', CASE WHEN iso.developed_emerging = 'Developed' THEN 'DM' ELSE 'EM' END)) AS market13
    , CASE WHEN iso.developed_emerging = 'Developed' THEN 'DM' ELSE 'EM' END AS EM_DM
    FROM   mdm.iso_cc_rollup_xref cc
    LEFT JOIN mdm.iso_country_code_xref iso
    ON iso.country_alpha2 = cc.country_alpha2
    WHERE  country_scenario = 'REGION_6'
    AND country_level_2 IS NOT NULL
    AND country_level_3 <> 'JP'
    AND market13 NOT IN ('UK&I-EM', 'NORTHERN EUROPE-EM', 'SOUTHERN EUROPE-EM' )
),

norm as (
    ---norm shipments with region 5 and updated brand info -----
SELECT 
    DISTINCT norm.platform_subset
    , hw.pl
    , substring(brand, 10, 5) AS toner_brand
    , hw.brand
    , hw.product_lifecycle_status
    , norm.region_5
    , hw.technology
    , norm.customer_engagement
    , concat(iso.market10,concat('-', CASE WHEN iso.developed_emerging = 'Developed' THEN 'DM' ELSE 'EM' END))::text AS market13
FROM   prod.norm_shipments_ce norm
INNER JOIN mdm.hardware_xref hw
       ON hw.platform_subset = norm.platform_subset
INNER JOIN mdm.iso_country_code_xref iso
       ON iso.country_alpha2 = norm.country_alpha2
WHERE  norm.version = (SELECT Max(version)
                       FROM   prod.norm_shipments_ce)
AND hw.technology IN ( 'LASER' )
),
 -- Pull NPI's from latest Norm Ships CE
npi as (
    SELECT DISTINCT platform_subset
    , toner_brand
    , brand
    , product_lifecycle_status
    , technology
    , customer_engagement
    , market13
    FROM   norm
    WHERE  product_lifecycle_status = 'N'

),
decay_curves as (
SELECT *
    FROM   (SELECT DISTINCT brand,
                Substring(brand, 10, 5)    AS toner_brand,
                split_name,
                geography,
                year,
                decay_m13.platform_subset,
                hw.product_lifecycle_status,
                isnull(round(value, 2), 0) AS val
            FROM   prod.decay_m13 decay_m13
            LEFT JOIN mdm.hardware_xref hw
            ON hw.platform_subset = decay_m13.platform_subset
            WHERE  hw.technology IN ( 'LASER' )
           --order by 1,2,3,4
            ) AS p
           PIVOT ( Sum(val)
                 FOR year IN ('YEAR_1',
                              'YEAR_2',
                              'YEAR_3',
                              'YEAR_4',
                              'YEAR_5',
                              'YEAR_6',
                              'YEAR_7',
                              'YEAR_8',
                              'YEAR_9',
                              'YEAR_10',
                              'YEAR_11',
                              'YEAR_12',
                              'YEAR_13',
                              'YEAR_14',
                              'YEAR_15',
                              'YEAR_16',
                              'YEAR_17',
                              'YEAR_18',
                              'YEAR_19',
                              'YEAR_20',
                              'YEAR_21',
                              'YEAR_22',
                              'YEAR_23',
                              'YEAR_24',
                              'YEAR_25',
                              'YEAR_26',
                              'YEAR_27',
                              'YEAR_28',
                              'YEAR_29',
                              'YEAR_30') ) AS pivottable),
decays as (
SELECT brand::text
    ,toner_brand::text
    ,split_name::text
    ,geography::text
    ,platform_subset::text
    ,product_lifecycle_status
    ,Isnull(year_1, 0)  AS year_1
    ,Isnull(year_2, 0)  AS year_2
    ,Isnull(year_3, 0)  AS year_3
    ,Isnull(year_4, 0)  AS year_4
    ,Isnull(year_5, 0)  AS year_5
    ,Isnull(year_6, 0)  AS year_6
    ,Isnull(year_7, 0)  AS year_7
    ,Isnull(year_8, 0)  AS year_8
    ,Isnull(year_9, 0)  AS year_9
    ,Isnull(year_10, 0) AS year_10
    ,Isnull(year_11, 0) AS year_11
    ,Isnull(year_12, 0) AS year_12
    ,Isnull(year_13, 0) AS year_13
    ,Isnull(year_14, 0) AS year_14
    ,Isnull(year_15, 0) AS year_15
    ,Isnull(year_16, 0) AS year_16
    ,Isnull(year_17, 0) AS year_17
    ,Isnull(year_18, 0) AS year_18
    ,Isnull(year_19, 0) AS year_19
    ,Isnull(year_20, 0) AS year_20
    ,Isnull(year_21, 0) AS year_21
    ,Isnull(year_22, 0) AS year_22
    ,Isnull(year_23, 0) AS year_23
    ,Isnull(year_24, 0) AS year_24
    ,Isnull(year_25, 0) AS year_25
    ,Isnull(year_26, 0) AS year_26
    ,Isnull(year_27, 0) AS year_27
    ,Isnull(year_28, 0) AS year_28
    ,Isnull(year_29, 0) AS year_29
    ,Isnull(year_30, 0) AS year_30
FROM  decay_curves
),
decay_profiles as (
SELECT DISTINCT brand
    , toner_brand
    , split_name
    , geography
    , product_lifecycle_status
    , year_1
    , year_2
    , year_3
    , year_4
    , year_5
    , year_6
    , year_7
    , year_8
    , year_9
    , year_10
    , year_11
    , year_12
    , year_13
    , year_14
    , year_15
    , year_16
    , year_17
    , year_18
    , year_19
    , year_20
    , year_21
    , year_22
    , year_23
    , year_24
    , year_25
    , year_26
    , year_27
    , year_28
    , year_29
    , year_30
    , count(platform_subset) AS platform_subset_count
                    
    FROM   decays
    GROUP  BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35
),
max_ps AS (
SELECT brand
    , toner_brand
    , split_name
    , geography
    , product_lifecycle_status
    , max(platform_subset_count) AS max_ps_count
    FROM   decay_profiles
    GROUP  BY 1,2,3,4,5),
toner_decays_npis as (
SELECT DISTINCT d.brand
    ,d.toner_brand
    ,d.split_name
    ,d.geography
    ,d.product_lifecycle_status
    ,m.max_ps_count
    ,year_1
    ,year_2
    ,year_3
    ,year_4
    ,year_5
    ,year_6
    ,year_7
    ,year_8
    ,year_9
    ,year_10
    ,year_11
    ,year_12
    ,year_13
    ,year_14
    ,year_15
    ,year_16
    ,year_17
    ,year_18
    ,year_19
    ,year_20
    ,year_21
    ,year_22
    ,year_23
    ,year_24
    ,year_25
    ,year_26
    ,year_27
    ,year_28
    ,year_29
    ,year_30
    FROM   decay_profiles d
    INNER JOIN max_ps m
    ON m.brand = d.brand
    AND m.geography = d.geography
    AND m.split_name = d.split_name
    AND m.max_ps_count = d.platform_subset_count
    AND m.product_lifecycle_status = d.product_lifecycle_status
    WHERE  d.product_lifecycle_status = 'N'
    GROUP  BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36),
npi_decays as (
SELECT concat('current ', concat(technology, ' decays'))::text AS record
    , technology
    , brand
    , toner_brand
    , product_lifecycle_status
    , platform_subset
    , '' AS market10
    , '' AS EM_DM
    , market13
    , substring(years, 6, 2) AS year
    , customer_engagement AS split_name
    , value
    FROM   (SELECT n.platform_subset,
               n.brand,
               n.toner_brand,
               n.product_lifecycle_status,
               n.technology,
               n.customer_engagement,
               n.market13,
               year_1,
               year_2,
               year_3,
               year_4,
               year_5,
               year_6,
               year_7,
               year_8,
               year_9,
               year_10,
               year_11,
               year_12,
               year_13,
               year_14,
               year_15,
               year_16,
               year_17,
               year_18,
               year_19,
               year_20,
               year_21,
               year_22,
               year_23,
               year_24,
               year_25,
               year_26,
               year_27,
               year_28,
               year_29,
               year_30
            FROM   npi n
            LEFT JOIN toner_decays_npis i
            ON i.brand = n.brand
            AND i.split_name = n.customer_engagement
            AND i.geography = n.market13)
           UNPIVOT (value
                   FOR years IN (year_1
                                ,year_2
                                ,year_3
                                ,year_4
                                ,year_5
                                ,year_6
                                ,year_7
                                ,year_8
                                ,year_9
                                ,year_10
                                ,year_11
                                ,year_12
                                ,year_13
                                ,year_14
                                ,year_15
                                ,year_16
                                ,year_17
                                ,year_18
                                ,year_19
                                ,year_20
                                ,year_21
                                ,year_22
                                ,year_23
                                ,year_24
                                ,year_25
                                ,year_26
                                ,year_27
                                ,year_28
                                ,year_29
                                ,year_30)) as pivottable ),
decay as (
SELECT *
FROM mdm.aul_upload_rev
WHERE  technology = 'LASER'
AND insert_ts = (SELECT Max(insert_ts) FROM mdm.aul_upload_rev WHERE  technology = 'LASER')),
---- Current Toner Decay curves
c_decay as (
SELECT record
    , decay.platform_subset
    , geography
    , substring(year, 6, 2) AS year
    , split_name
    , value
    , avg_printer_life
    --, active
    --, active_at
    --, inactive_at
    , decay.load_date
    , decay.version
    , decay.official
    , geography_grain
FROM prod.decay decay
INNER JOIN mdm.hardware_xref hw
ON hw.platform_subset = decay.platform_subset
WHERE  hw.technology = 'LASER'
AND decay.official = 1),
new_auls as (
SELECT 'NEW AULs'::text AS record
    , norm.toner_brand::text
    , norm.brand::text
    , norm.platform_subset::text
    , geo.market10::text
    , geo.em_dm
    , geo.market13::text
    , norm.region_5::text
    , year::int
    , split_name::text
    , norm.product_lifecycle_status
    , decay.value
    , NULL AS AUL
    , cast(decay.insert_ts AS DATE) AS load_date
    , decay.version
    , 1 AS official
    , norm.technology::text
FROM  norm norm
INNER JOIN geo geo
ON norm.region_5 = geo.region_5
INNER JOIN decay decay
ON norm.brand = decay.product
AND decay.geography = geo.region_6
WHERE  norm.platform_subset NOT IN ('APOLLO MFP E4'
        , 'BEIJING 40 CCT'
        , 'BIGBANG E4'
        , 'BIGBANGPLUS E4'
        , 'CARDINAL E4'
        , 'CARDINAL2 E4'
        , 'CLOVER E4'
        , 'COSMOS-M/R ED'
        , 'CYGNUS E0'
        , 'DALI E0'
        , 'DAVINCI E0'
        , 'DOLPHIN E4'
        , 'DOVE E4'
        , 'EAGLE/EAGLEPLUS E4'
        , 'EIGER-R E4'
        , 'EIGER E4'
        , 'ELBERT E4'
        , 'ELBRUZ/MCKINLEY2 E4'
        , 'GOGH E4'
        , 'HAWK12/HAWK14 E4'
        , 'HAWK16 E4'
        , 'HUMMINGBIRD E4'
        , 'HUMMINGBIRD2 E4'
        , 'HUMMINGBIRDVE E4'
        , 'IRIS E4'
        , 'KESTREL E0'
        , 'LILY E0'
        , 'LOGAN E4'
        , 'MCKINLEY/MCKINLEYPLUS/ZEUS E4'
        , 'MERCURY E4'
        , 'MILLET E4'
        , 'ML-OPT E4'
        , 'MONTBLANC E4'
        , 'NIKE E4'
        , 'NIKE2LOW/NIKE2HIGH/ROCKY E4'
        , 'ORION E4'
        , 'PEACOCK E4'
        , 'PHOENIX E4'
        , 'PHOENIX2 E4'
        , 'PLOVER-P E4'
        , 'PLOVER E4'
        , 'ROBIN E4'
        , 'ROCKY2/ROCKY1PLUS E4'
        , 'ROSE E0'
        , 'RUBENS E4'
        , 'SKYLARK E4'
        , 'SWALLOW E4'
        , 'SWIFT E4'
        , 'WHITNEYPLUS E4'
        , 'BLUEBIRD E4'
        , 'BLUEMOUNTAIN E4'
        , 'BLUEJAY E4'
        , 'EGMONT E4'
        , 'C430 E4'
        , 'C480 E4'
        , 'CARLSEN E4'
        , 'VIOLET E4'
        , 'MAPLE E4'
        , 'BATIAN-N INT E4'
        , 'KINGBIRD-N INT E4'
        , 'BATIAN-N SEP E4'
        , 'KINGBIRD-N SEP E4'
        , 'BATIAN E4'
        , 'KINGBIRD E4'
        , 'AZALEA E4'
        , 'KLIMT E4'
        , 'M2010 E4'
        , 'M2060 E4'
        , 'M2020 E4'
        , 'M2070 E4'
        , 'TULIP E4'
        , 'STANLEY E4'
        , 'LHOTSE E4'
        , 'SANDPIPER E4'
        , 'SWAN E4'
        , 'TISSOT E4'
        , 'ZINNIA E4'
        , 'WHITNEY2 E4'
        , 'WHITNEY3 E4'
        , 'BEIJING 40'
        , 'BEIJING 45'
        , 'C2620 E0'
        , 'C2670 E0'
        , 'C3010 E0'
        , 'C3060 E0'
        , 'C4010 E0'
        , 'C4060 E0'
        , 'CAPELLA ED'
        , 'CEZANNE/CEZANNE-R E0'
        , 'COSMOS-C/R ED'
        , 'DALI-R E0'
        , 'LILY-R E0'
        , 'ELBERT2/ELBERT-P E0'
        , 'EVERGREEN E0'
        , 'HARRIER E0'
        , 'JUNGFRAU E0'
        , 'K3250 ED'
        , 'K4350 ED'
        , 'K7600 ED'
        , 'KESTREL2/STORK E0'
        , 'M4030 E0'
        , 'M4080 E0'
        , 'M4530(IT) E0'
        , 'M4580(IT) E0'
        , 'M4580(OA) E0'
        , 'M5370 E0'
        , 'MAGPIE-N INT 33 E0'
        , 'RAINIER-N INT 33 E0'
        , 'MAGPIE-N INT 38 E0'
        , 'RAINIER-N INT 38 E0'
        , 'MAGPIE-N INT 40 E0'
        , 'RAINIER-N INT 40 E0'
        , 'MAGPIE-N SEP 33 E0'
        , 'RAINIER-N SEP 33 E0'
        , 'MAGPIE-N SEP 38 E0'
        , 'RAINIER-N SEP 38 E0'
        , 'MAGPIE-N SEP 40 E0'
        , 'RAINIER-N SEP 40 E0'
        , 'MAGPIE-PLUS E0'
        , 'MAGPIE E0'
        , 'RAINIER E0'
        , 'PETREL E0'
        , 'POLARIS-C ED'
        , 'POLARIS-M ED'
        , 'ROSE-R E0'
        , 'ROUSSEAU E0'
        , 'SCARLET E0'
        , 'RUSHMORE/RUSHMORE-M E0'
        , 'X3220 ED'
        , 'X4300 ED'
        , 'X7600 ED'
        , 'M3015 E4'
        , 'M3065 E4' ) 
AND norm.pl NOT IN ( 'G8', 'IT' )
),
union_step as (
SELECT *
FROM   new_auls aul
WHERE  aul.toner_brand IN ( 'DEPT', 'SWT-H' )
AND aul.product_lifecycle_status <> 'N' 
UNION ALL
 --- Pull new AUL's for all NPI's ----
SELECT *
FROM   (SELECT 'NEW AULs'::text AS record
            , n.toner_brand::text
            , n.brand::text
            , n.platform_subset::text
            , geo.market10::text
            , geo.em_dm
            , geo.market13::text
            , geo.region_5::text
            , year::int
            , split_name::text
            , n.product_lifecycle_status
            , n.value
            , NULL AS AUL
            , SYSDATE AS load_date
            ,'' AS version
            , 1 AS official
            , n.technology::text
        FROM   npi_decays n
        INNER JOIN geo geo
        ON n.market13 = geo.market13 ) AS aul_2
WHERE  aul_2.product_lifecycle_status = 'N' 
UNION ALL
------ PULL NEW AULs for PLE-L in NA, NW E, CE, SE, GA -------
SELECT *
FROM new_auls aul  
WHERE  aul.toner_brand IN ( 'PLE-L', 'PLE-H' )
AND aul.market10 IN ( 'North America', 'Central Europe','UK&I','Northern Europe','Southern Europe', 'Greater Asia' )
AND aul.product_lifecycle_status <> 'N' 
UNION ALL
---- PULL OLD AULs for for SWT-L & WG -------
SELECT DISTINCT *
FROM   (SELECT 'CURRENT AULs'::text AS record
            , norm.toner_brand::text
            , norm.brand::text
            , norm.platform_subset::text
            , geo.market10::text
            , geo.em_dm
            , geo.market13::text
            , norm.region_5::text
            , year::int
            , split_name::text
            , norm.product_lifecycle_status
            , c_decay.value
            , NULL AS AUL
            , c_decay.load_date
            , c_decay.version
            , 1 AS official
            , norm.technology::text
        FROM   norm norm
        INNER JOIN geo geo
        ON norm.region_5 = geo.region_5
        INNER JOIN c_decay c_decay
        ON norm.platform_subset = c_decay.platform_subset
        AND norm.region_5 = c_decay.geography
        WHERE  norm.platform_subset NOT IN ('APOLLO MFP E4'
                , 'BEIJING 40 CCT'
                , 'BIGBANG E4'
                , 'BIGBANGPLUS E4'
                , 'CARDINAL E4'
                , 'CARDINAL2 E4'
                , 'CLOVER E4'
                , 'COSMOS-M/R ED'
                , 'CYGNUS E0' 
                , 'DALI E0'
                , 'DAVINCI E0'
                , 'DOLPHIN E4'
                , 'DOVE E4'
                , 'EAGLE/EAGLEPLUS E4'
                , 'EIGER-R E4'
                , 'EIGER E4'
                , 'ELBERT E4'
                , 'ELBRUZ/MCKINLEY2 E4'
                , 'GOGH E4'
                , 'HAWK12/HAWK14 E4'
                , 'HAWK16 E4'
                , 'HUMMINGBIRD E4'
                , 'HUMMINGBIRD2 E4'
                , 'HUMMINGBIRDVE E4'
                , 'IRIS E4'
                , 'KESTREL E0'
                , 'LILY E0'
                , 'LOGAN E4'
                , 'MCKINLEY/MCKINLEYPLUS/ZEUS E4'
                , 'MERCURY E4'
                , 'MILLET E4'
                , 'ML-OPT E4'
                , 'MONTBLANC E4'
                , 'NIKE E4'
                , 'NIKE2LOW/NIKE2HIGH/ROCKY E4'
                , 'ORION E4'
                , 'PEACOCK E4'
                , 'PHOENIX E4'
                , 'PHOENIX2 E4'
                , 'PLOVER-P E4'
                , 'PLOVER E4'
                , 'ROBIN E4'
                , 'ROCKY2/ROCKY1PLUS E4'
                , 'ROSE E0'
                , 'RUBENS E4'
                , 'SKYLARK E4'
                , 'SWALLOW E4'
                , 'SWIFT E4'
                , 'WHITNEYPLUS E4'
                , 'BLUEBIRD E4'
                , 'BLUEMOUNTAIN E4'
                , 'BLUEJAY E4'
                , 'EGMONT E4' 
                , 'C430 E4'
                , 'C480 E4'
                , 'CARLSEN E4'
                , 'VIOLET E4'
                , 'MAPLE E4'
                , 'BATIAN-N INT E4'
                , 'KINGBIRD-N INT E4'
                , 'BATIAN-N SEP E4'
                , 'KINGBIRD-N SEP E4'
                , 'BATIAN E4'
                , 'KINGBIRD E4'
                , 'AZALEA E4' 
                , 'KLIMT E4'
                , 'M2010 E4'
                , 'M2060 E4'
                , 'M2020 E4' 
                , 'M2070 E4'
                , 'TULIP E4'
                , 'STANLEY E4'
                , 'LHOTSE E4'
                , 'SANDPIPER E4'
                , 'SWAN E4'
                , 'TISSOT E4'
                , 'ZINNIA E4'
                , 'WHITNEY2 E4'
                , 'WHITNEY3 E4'
                , 'BEIJING 40'
                , 'BEIJING 45'
                , 'C2620 E0'
                , 'C2670 E0'
                , 'C3010 E0'
                , 'C3060 E0'
                , 'C4010 E0'
                , 'C4060 E0'
                , 'CAPELLA ED'
                , 'CEZANNE/CEZANNE-R E0'
                , 'COSMOS-C/R ED'
                , 'DALI-R E0'
                , 'LILY-R E0'
                , 'ELBERT2/ELBERT-P E0'
                , 'EVERGREEN E0'
                , 'HARRIER E0'
                , 'JUNGFRAU E0'
                , 'K3250 ED'
                , 'K4350 ED'
                , 'K7600 ED'
                , 'KESTREL2/STORK E0'
                , 'M4030 E0'
                , 'M4080 E0'
                , 'M4530(IT) E0'
                , 'M4580(IT) E0'
                , 'M4580(OA) E0'
                , 'M5370 E0'
                , 'MAGPIE-N INT 33 E0'
                , 'RAINIER-N INT 33 E0'
                , 'MAGPIE-N INT 38 E0'
                , 'RAINIER-N INT 38 E0'
                , 'MAGPIE-N INT 40 E0'
                , 'RAINIER-N INT 40 E0'
                , 'MAGPIE-N SEP 33 E0'
                , 'RAINIER-N SEP 33 E0'
                , 'MAGPIE-N SEP 38 E0'
                , 'RAINIER-N SEP 38 E0'
                , 'MAGPIE-N SEP 40 E0'
                , 'RAINIER-N SEP 40 E0'
                , 'MAGPIE-PLUS E0'
                , 'MAGPIE E0'
                , 'RAINIER E0'
                , 'PETREL E0'
                , 'POLARIS-C ED'
                , 'POLARIS-M ED'
                , 'ROSE-R E0'
                , 'ROUSSEAU E0'
                , 'SCARLET E0'
                , 'RUSHMORE/RUSHMORE-M E0'
                , 'X3220 ED'
                , 'X4300 ED'
                , 'X7600 ED'
                , 'M3015 E4'
                , 'M3065 E4' )
                AND norm.pl NOT IN ( 'G8', 'IT' )) aul
WHERE  aul.toner_brand IN ( 'SWT-L', 'WG' )
AND aul.product_lifecycle_status <> 'N'
UNION ALL
---- PULL OLD AULs for for PLE-L & PLE-H  for LA, ISE, GC, GI, -------
SELECT DISTINCT *
FROM   (SELECT 'CURRENT AULs'::text AS record
            , norm.toner_brand::text
            , norm.brand::text
            , norm.platform_subset::text
            , geo.market10::text
            , geo.em_dm
            , geo.market13::text
            , norm.region_5::text
            , year::int
            , split_name::text
            , norm.product_lifecycle_status
            , c_decay.value
            , NULL AS AUL
            , c_decay.load_date
            , c_decay.version
            , 1 AS official
            , norm.technology::text
        FROM   norm norm
        INNER JOIN geo geo
        ON norm.region_5 = geo.region_5
        INNER JOIN c_decay c_decay
        ON norm.platform_subset = c_decay.platform_subset
        AND norm.region_5 = c_decay.geography
        WHERE  norm.platform_subset NOT IN ('APOLLO MFP E4' 
                , 'BEIJING 40 CCT'
                , 'BIGBANG E4'
                , 'BIGBANGPLUS E4'
                , 'CARDINAL E4'
                , 'CARDINAL2 E4'
                , 'CLOVER E4'
                , 'COSMOS-M/R ED'
                , 'CYGNUS E0'
                , 'DALI E0'
                , 'DAVINCI E0'
                , 'DOLPHIN E4'
                , 'DOVE E4'
                , 'EAGLE/EAGLEPLUS E4'
                , 'EIGER-R E4'
                , 'EIGER E4'
                , 'ELBERT E4'
                , 'ELBRUZ/MCKINLEY2 E4'
                , 'GOGH E4'
                , 'HAWK12/HAWK14 E4'
                , 'HAWK16 E4'
                , 'HUMMINGBIRD E4'
                , 'HUMMINGBIRD2 E4'
                , 'HUMMINGBIRDVE E4'
                , 'IRIS E4'
                , 'KESTREL E0'
                , 'LILY E0'
                , 'LOGAN E4'
                , 'MCKINLEY/MCKINLEYPLUS/ZEUS E4'
                , 'MERCURY E4'
                , 'MILLET E4'
                , 'ML-OPT E4'
                , 'MONTBLANC E4'
                , 'NIKE E4'
                , 'NIKE2LOW/NIKE2HIGH/ROCKY E4'
                , 'ORION E4'
                , 'PEACOCK E4'
                , 'PHOENIX E4'
                , 'PHOENIX2 E4'
                , 'PLOVER-P E4'
                , 'PLOVER E4'
                , 'ROBIN E4'
                , 'ROCKY2/ROCKY1PLUS E4'
                , 'ROSE E0'
                , 'RUBENS E4'
                , 'SKYLARK E4'
                , 'SWALLOW E4'
                , 'SWIFT E4'
                , 'WHITNEYPLUS E4'
                , 'BLUEBIRD E4'
                , 'BLUEMOUNTAIN E4'
                , 'BLUEJAY E4'
                , 'EGMONT E4'
                , 'C430 E4'
                , 'C480 E4'
                , 'CARLSEN E4'
                , 'VIOLET E4'
                , 'MAPLE E4'
                , 'BATIAN-N INT E4'
                , 'KINGBIRD-N INT E4'
                , 'BATIAN-N SEP E4'
                , 'KINGBIRD-N SEP E4'
                , 'BATIAN E4'
                , 'KINGBIRD E4'
                , 'AZALEA E4'
                , 'KLIMT E4'
                , 'M2010 E4'
                , 'M2060 E4'
                , 'M2020 E4'
                , 'M2070 E4'
                , 'TULIP E4'
                , 'STANLEY E4'
                , 'LHOTSE E4'
                , 'SANDPIPER E4'
                , 'SWAN E4'
                , 'TISSOT E4'
                , 'ZINNIA E4'
                , 'WHITNEY2 E4'
                , 'WHITNEY3 E4'
                , 'BEIJING 40'
                , 'BEIJING 45'
                , 'C2620 E0'
                , 'C2670 E0'
                , 'C3010 E0'
                , 'C3060 E0' 
                , 'C4010 E0'
                , 'C4060 E0'
                , 'CAPELLA ED'
                , 'CEZANNE/CEZANNE-R E0'
                , 'COSMOS-C/R ED'
                , 'DALI-R E0' 
                , 'LILY-R E0'
                , 'ELBERT2/ELBERT-P E0'
                , 'EVERGREEN E0'
                , 'HARRIER E0'
                , 'JUNGFRAU E0'
                , 'K3250 ED'
                , 'K4350 ED'
                , 'K7600 ED'
                , 'KESTREL2/STORK E0'
                , 'M4030 E0'
                , 'M4080 E0'
                , 'M4530(IT) E0'
                , 'M4580(IT) E0'
                , 'M4580(OA) E0'
                , 'M5370 E0'
                , 'MAGPIE-N INT 33 E0'
                , 'RAINIER-N INT 33 E0'
                , 'MAGPIE-N INT 38 E0'
                , 'RAINIER-N INT 38 E0'
                , 'MAGPIE-N INT 40 E0'
                , 'RAINIER-N INT 40 E0'
                , 'MAGPIE-N SEP 33 E0'
                , 'RAINIER-N SEP 33 E0'
                , 'MAGPIE-N SEP 38 E0'
                , 'RAINIER-N SEP 38 E0'
                , 'MAGPIE-N SEP 40 E0'
                , 'RAINIER-N SEP 40 E0'
                , 'MAGPIE-PLUS E0'
                , 'MAGPIE E0'
                , 'RAINIER E0'
                , 'PETREL E0'
                , 'POLARIS-C ED'
                , 'POLARIS-M ED'
                , 'ROSE-R E0'
                , 'ROUSSEAU E0'
                , 'SCARLET E0'
                , 'RUSHMORE/RUSHMORE-M E0'
                , 'X3220 ED'
                , 'X4300 ED'
                , 'X7600 ED'
                , 'M3015 E4'
                , 'M3065 E4')
        AND norm.pl NOT IN ( 'G8', 'IT' )) aul
WHERE  aul.toner_brand IN ( 'PLE-L', 'PLE-H' )
AND aul.market10 IN ( 'Latin America','ISE','Greater China','India SL & BL' )
AND aul.product_lifecycle_status <> 'N'
UNION ALL
---- PULL Legacy AUL's for Dragon Products : PL G8 and IT ----
SELECT DISTINCT *
FROM   (SELECT 'CURRENT AULs'::text AS record
            , norm.toner_brand::text
            , norm.brand::text
            , norm.platform_subset::text
            , geo.market10::text
            , geo.em_dm
            , geo.market13::text
            , norm.region_5::text
            , year::int
            , split_name::text
            , norm.product_lifecycle_status
            , c_decay.value
            , NULL AS AUL
            , c_decay.load_date
            , c_decay.version
            , 1 AS official
            , norm.technology::text
        FROM   norm norm
        INNER JOIN geo geo
        ON norm.region_5 = geo.region_5
        INNER JOIN c_decay c_decay
        ON norm.platform_subset = c_decay.platform_subset
        AND norm.region_5 = c_decay.geography
        WHERE  norm.platform_subset NOT IN ('APOLLO MFP E4'
                , 'BEIJING 40 CCT'
                , 'BIGBANG E4'
                , 'BIGBANGPLUS E4'
                , 'CARDINAL E4'
                , 'CARDINAL2 E4'
                , 'CLOVER E4'
                , 'COSMOS-M/R ED'
                , 'CYGNUS E0' 
                , 'DALI E0'
                , 'DAVINCI E0' 
                , 'DOLPHIN E4'
                , 'DOVE E4'
                , 'EAGLE/EAGLEPLUS E4'
                , 'EIGER-R E4'
                , 'EIGER E4'
                , 'ELBERT E4'
                , 'ELBRUZ/MCKINLEY2 E4'
                , 'GOGH E4'
                , 'HAWK12/HAWK14 E4'
                , 'HAWK16 E4'
                , 'HUMMINGBIRD E4'
                , 'HUMMINGBIRD2 E4'
                , 'HUMMINGBIRDVE E4'
                , 'IRIS E4' 
                , 'KESTREL E0'
                , 'LILY E0'
                , 'LOGAN E4'
                , 'MCKINLEY/MCKINLEYPLUS/ZEUS E4'
                , 'MERCURY E4'
                , 'MILLET E4'
                , 'ML-OPT E4'
                , 'MONTBLANC E4'
                , 'NIKE E4'
                , 'NIKE2LOW/NIKE2HIGH/ROCKY E4'
                , 'ORION E4'
                , 'PEACOCK E4'
                , 'PHOENIX E4'
                , 'PHOENIX2 E4'
                , 'PLOVER-P E4'
                , 'PLOVER E4'
                , 'ROBIN E4'
                , 'ROCKY2/ROCKY1PLUS E4'
                , 'ROSE E0'
                , 'RUBENS E4'
                , 'SKYLARK E4'
                , 'SWALLOW E4'
                , 'SWIFT E4'
                , 'WHITNEYPLUS E4'
                , 'BLUEBIRD E4'
                , 'BLUEMOUNTAIN E4'
                , 'BLUEJAY E4'
                , 'EGMONT E4'
                , 'C430 E4'
                , 'C480 E4'
                , 'CARLSEN E4'
                , 'VIOLET E4'
                , 'MAPLE E4'
                , 'BATIAN-N INT E4'
                , 'KINGBIRD-N INT E4'
                , 'BATIAN-N SEP E4'
                , 'KINGBIRD-N SEP E4'
                , 'BATIAN E4'
                , 'KINGBIRD E4'
                , 'AZALEA E4'
                , 'KLIMT E4'
                , 'M2010 E4'
                , 'M2060 E4'
                , 'M2020 E4'
                , 'M2070 E4'
                , 'TULIP E4'
                , 'STANLEY E4'
                , 'LHOTSE E4'
                , 'SANDPIPER E4'
                , 'SWAN E4'
                , 'TISSOT E4'
                , 'ZINNIA E4'
                , 'WHITNEY2 E4'
                , 'WHITNEY3 E4'
                , 'BEIJING 40'
                , 'BEIJING 45'
                , 'C2620 E0'
                , 'C2670 E0'
                , 'C3010 E0'
                , 'C3060 E0'
                , 'C4010 E0'
                , 'C4060 E0'
                , 'CAPELLA ED'
                , 'CEZANNE/CEZANNE-R E0'
                , 'COSMOS-C/R ED'
                , 'DALI-R E0' 
                , 'LILY-R E0'
                , 'ELBERT2/ELBERT-P E0'
                , 'EVERGREEN E0'
                , 'HARRIER E0'
                , 'JUNGFRAU E0'
                , 'K3250 ED' 
                , 'K4350 ED'
                , 'K7600 ED'
                , 'KESTREL2/STORK E0'
                , 'M4030 E0' 
                , 'M4080 E0'
                , 'M4530(IT) E0'
                , 'M4580(IT) E0'
                , 'M4580(OA) E0'
                , 'M5370 E0'
                , 'MAGPIE-N INT 33 E0'
                , 'RAINIER-N INT 33 E0'
                , 'MAGPIE-N INT 38 E0'
                , 'RAINIER-N INT 38 E0'
                , 'MAGPIE-N INT 40 E0'
                , 'RAINIER-N INT 40 E0'
                , 'MAGPIE-N SEP 33 E0'
                , 'RAINIER-N SEP 33 E0'
                , 'MAGPIE-N SEP 38 E0'
                , 'RAINIER-N SEP 38 E0'
                , 'MAGPIE-N SEP 40 E0'
                , 'RAINIER-N SEP 40 E0'
                , 'MAGPIE-PLUS E0'
                , 'MAGPIE E0'
                , 'RAINIER E0'
                , 'PETREL E0'
                , 'POLARIS-C ED'
                , 'POLARIS-M ED'
                , 'ROSE-R E0'
                , 'ROUSSEAU E0'
                , 'SCARLET E0'
                , 'RUSHMORE/RUSHMORE-M E0'
                , 'X3220 ED' 
                , 'X4300 ED'
                , 'X7600 ED'
                , 'M3015 E4'
                , 'M3065 E4')
        AND norm.pl IN ( 'G8', 'IT' )
        AND norm.product_lifecycle_status <> 'N' ) aul 
UNION ALL 
---- PULL Legacy AUL's for S-Print
SELECT DISTINCT *
FROM   (SELECT 'CURRENT AULs'::text AS record
            , norm.toner_brand::text
            , norm.brand::text
            , norm.platform_subset::text
            , geo.market10::text
            , geo.em_dm
            , geo.market13::text
            , norm.region_5::text
            , year::int
            , split_name::text
            , norm.product_lifecycle_status
            , c_decay.value
            , NULL AS AUL
            , c_decay.load_date
            , c_decay.version
            , 1 AS official
            , norm.technology::text
        FROM  norm norm
        INNER JOIN geo geo
        ON norm.region_5 = geo.region_5
        INNER JOIN c_decay c_decay
        ON norm.platform_subset = c_decay.platform_subset
        AND norm.region_5 = c_decay.geography
        WHERE  norm.platform_subset IN ('APOLLO MFP E4'
                , 'BEIJING 40 CCT'
                , 'BIGBANG E4'
                , 'BIGBANGPLUS E4'
                , 'CARDINAL E4'
                , 'CARDINAL2 E4'
                , 'CLOVER E4'
                , 'COSMOS-M/R ED'
                , 'CYGNUS E0'
                , 'DALI E0'
                , 'DAVINCI E0'
                , 'DOLPHIN E4'
                , 'DOVE E4'
                , 'EAGLE/EAGLEPLUS E4'
                , 'EIGER-R E4'
                , 'EIGER E4'
                , 'ELBERT E4'
                , 'ELBRUZ/MCKINLEY2 E4'
                , 'GOGH E4'
                , 'HAWK12/HAWK14 E4'
                , 'HAWK16 E4'
                , 'HUMMINGBIRD E4'
                , 'HUMMINGBIRD2 E4'
                , 'HUMMINGBIRDVE E4'
                , 'IRIS E4'
                , 'KESTREL E0'
                , 'LILY E0'
                , 'LOGAN E4'
                , 'MCKINLEY/MCKINLEYPLUS/ZEUS E4'
                , 'MERCURY E4'
                , 'MILLET E4'
                , 'ML-OPT E4'
                , 'MONTBLANC E4'
                , 'NIKE E4'
                , 'NIKE2LOW/NIKE2HIGH/ROCKY E4'
                , 'ORION E4'
                , 'PEACOCK E4'
                , 'PHOENIX E4'
                , 'PHOENIX2 E4'
                , 'PLOVER-P E4'
                , 'PLOVER E4'
                , 'ROBIN E4'
                , 'ROCKY2/ROCKY1PLUS E4'
                , 'ROSE E0'
                , 'RUBENS E4'
                , 'SKYLARK E4'
                , 'SWALLOW E4'
                , 'SWIFT E4'
                , 'WHITNEYPLUS E4'
                , 'BLUEBIRD E4'
                , 'BLUEMOUNTAIN E4'
                , 'BLUEJAY E4'
                , 'EGMONT E4' 
                , 'C430 E4'
                , 'C480 E4'
                , 'CARLSEN E4'
                , 'VIOLET E4'
                , 'MAPLE E4'
                , 'BATIAN-N INT E4'
                , 'KINGBIRD-N INT E4'
                , 'BATIAN-N SEP E4'
                , 'KINGBIRD-N SEP E4'
                , 'BATIAN E4'
                , 'KINGBIRD E4'
                , 'AZALEA E4' 
                , 'KLIMT E4'
                , 'M2010 E4'
                , 'M2060 E4'
                , 'M2020 E4'
                , 'M2070 E4'
                , 'TULIP E4'
                , 'STANLEY E4'
                , 'LHOTSE E4'
                , 'SANDPIPER E4'
                , 'SWAN E4'
                , 'TISSOT E4'
                , 'ZINNIA E4'
                , 'WHITNEY2 E4'
                , 'WHITNEY3 E4'
                , 'BEIJING 40'
                , 'BEIJING 45'
                , 'C2620 E0'
                , 'C2670 E0'
                , 'C3010 E0'
                , 'C3060 E0'
                , 'C4010 E0'
                , 'C4060 E0'
                , 'CAPELLA ED'
                , 'CEZANNE/CEZANNE-R E0'
                , 'COSMOS-C/R ED'
                , 'DALI-R E0'
                , 'LILY-R E0'
                , 'ELBERT2/ELBERT-P E0'
                , 'EVERGREEN E0'
                , 'HARRIER E0'
                , 'JUNGFRAU E0'
                , 'K3250 ED'
                , 'K4350 ED'
                , 'K7600 ED'
                , 'KESTREL2/STORK E0'
                , 'M4030 E0'
                , 'M4080 E0'
                , 'M4530(IT) E0'
                , 'M4580(IT) E0'
                , 'M4580(OA) E0'
                , 'M5370 E0'
                , 'MAGPIE-N INT 33 E0'
                , 'RAINIER-N INT 33 E0'
                , 'MAGPIE-N INT 38 E0'
                , 'RAINIER-N INT 38 E0'
                , 'MAGPIE-N INT 40 E0'
                , 'RAINIER-N INT 40 E0'
                , 'MAGPIE-N SEP 33 E0'
                , 'RAINIER-N SEP 33 E0'
                , 'MAGPIE-N SEP 38 E0'
                , 'RAINIER-N SEP 38 E0'
                , 'MAGPIE-N SEP 40 E0'
                , 'RAINIER-N SEP 40 E0'
                , 'MAGPIE-PLUS E0'
                , 'MAGPIE E0'
                , 'RAINIER E0'
                , 'PETREL E0'
                , 'POLARIS-C ED'
                , 'POLARIS-M ED'
                , 'ROSE-R E0'
                , 'ROUSSEAU E0'
                , 'SCARLET E0'
                , 'RUSHMORE/RUSHMORE-M E0'
                , 'X3220 ED'
                , 'X4300 ED'
                , 'X7600 ED'
                , 'M3015 E4'
                , 'M3065 E4' )
        AND norm.pl NOT IN ( 'G8', 'IT' )
        AND norm.product_lifecycle_status <> 'N') aul)
 SELECT record
      , technology
      , brand
      , product_lifecycle_status
      , platform_subset
      , market10
      , em_dm
      , market13
      , year
      , split_name
      , value
      , SYSDATE AS load_date from union_step"""

# COMMAND ----------

laser_decay_df = read_redshift_to_df(configs) \
  .option("query",laser_decay_curve) \
  .load()
print("Loaded laser decay curves data in df")

# COMMAND ----------

ink_laser_decay_union = ink_decay_df.unionAll(laser_decay_df)
print("Unioning ink and laser decay curves data")

# COMMAND ----------

write_df_to_redshift(configs,ink_laser_decay_union,"staging.ink_toner_combined_decay_curves_final","overwrite")
print("Finished writing to redshift")
