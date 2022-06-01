# Databricks notebook source
# MAGIC %md
# MAGIC # IB IINK-LTP Trends

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Documentation
# MAGIC 
# MAGIC **OBJECTIVE:** to generate appropriate trends for the I-INK LTP
# MAGIC 
# MAGIC *Algorigthm development:*
# MAGIC ```text
# MAGIC algorithm iink-ltf-ib is
# MAGIC     input: ce_splits.split,
# MAGIC            norm_ships.units,
# MAGIC            iink_ltf.p1_cumulative,
# MAGIC            iink_ltf.p2_cumulative,
# MAGIC            iink_ltf.cumulative_enrollees
# MAGIC            
# MAGIC     output: updated IB datapoint
# MAGIC         
# MAGIC     for each ltf_iink.month_begin do
# MAGIC         ltf_enroll_split = ltf_iink.cum_enrollees * ce_split.split
# MAGIC         p2_correction = ltf_enroll_split - 
# MAGIC                             (ce_split.split * norm_ships.units)
# MAGIC           
# MAGIC     if printer has stopped shipping then
# MAGIC         decay out last datapoint by .05 per year
# MAGIC         
# MAGIC     if printer is shipping then
# MAGIC         remove p2_correction from trad IB
# MAGIC         
# MAGIC     return output 
# MAGIC ```
# MAGIC 
# MAGIC **Synopsis:**
# MAGIC + we need more information using norm_ships CKs to determine which printers should decay
# MAGIC + we will have to keep 
# MAGIC 
# MAGIC *Current algorithm:*
# MAGIC ```text
# MAGIC algorithm iink-ltf-ib
# MAGIC     input: iink_stf
# MAGIC     
# MAGIC     output: iink-ltf-ib
# MAGIC     
# MAGIC     for each iink_stf.composite_key do
# MAGIC         find the avg cumulative enrollees for the last month of the stf time box
# MAGIC         find the avg p2 cumulative enrollees for the last month of the stf time box
# MAGIC         apply these datapoints until the end of the norm shipments time box for each composite key
# MAGIC         
# MAGIC         weight the ltp cumulative enrollees using the above datapoints
# MAGIC         weight the ltp p2 cumulative enrollees using the above datapoints
# MAGIC         
# MAGIC         remove the p2 cumulative enrollees from trad ib        
# MAGIC ```
# MAGIC 
# MAGIC *issues:*
# MAGIC + all stf composite keys are allocated ltp IB (all printers increase in the ltf time box)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook setup

# COMMAND ----------

# MAGIC %run "../common/configs"

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare IINK-STF to norm ships - iter1

# COMMAND ----------

compare1 = """
with iink as 
(
  SELECT month_begin
      , geography_grain
      , geography
      , country_alpha2
      , split_name
      , platform_subset
      , p2_attach
      , ib
  FROM "stage"."ib_03_iink_complete"
  WHERE 1=1
      AND CAST(month_begin AS DATE) > CAST('2022-10-01' AS DATE)
)

, ns as 
(
  select ns.country_alpha2
    , ns.platform_subset
    , max(ns.cal_date) as max_cal_date
  from prod.norm_shipments as ns
  join mdm.hardware_xref as hw 
    on upper(hw.platform_subset) = upper(ns.platform_subset)
  where version = (select max(version) from prod.norm_shipments)
    and upper(hw.technology) in ('INK')
  group by ns.country_alpha2
    , ns.platform_subset
)

, test as 
(
select distinct iink.platform_subset
  , iink.country_alpha2
  , iink.split_name
  , ns.max_cal_date
from iink
left join ns
  on upper(ns.platform_subset) = upper(iink.platform_subset)
  and upper(ns.country_alpha2) = upper(iink.country_alpha2)
where 1=1
  and upper(iink.split_name) = 'I-INK'
)

select case when max_cal_date < cast('2022-10-01' as date) then 'to-decay'
            when max_cal_date >= cast('2022-10-01' as date) then 'to-allocate-ltp'
            else 'error' end as test_column
  , count(1) as row_count
from test
group by case when max_cal_date < cast('2022-10-01' as date) then 'to-decay'
            when max_cal_date >= cast('2022-10-01' as date) then 'to-allocate-ltp'
            else 'error' end
order by 1
"""

# COMMAND ----------

compare1_df = read_redshift_to_df(configs) \
  .option("query", compare1) \
  .load()

compare1_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results
# MAGIC 
# MAGIC Some notes on iteration 1:
# MAGIC + error records above highlight iink platform_subset/country combinations that do NOT exist in normalized shipments
# MAGIC + update the script to get the max ship date by platform_subset and market10 so that we can add combinations to the different categories

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare IINK-STF to norm ships - iter2

# COMMAND ----------

compare2 = """
WITH iink AS
    (SELECT month_begin
          , geography_grain
          , geography
          , country_alpha2
          , split_name
          , platform_subset
          , p2_attach
          , ib
     FROM "stage"."ib_03_iink_complete"
     WHERE 1 = 1
       AND CAST(month_begin AS DATE) > CAST('2022-10-01' AS DATE))

   , ns_country AS
    (SELECT ns.country_alpha2
          , ns.platform_subset
          , MAX(ns.cal_date) AS max_cal_date
     FROM prod.norm_shipments AS ns
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(ns.platform_subset)
     WHERE ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND UPPER(hw.technology) IN ('INK')
     GROUP BY ns.country_alpha2
            , ns.platform_subset)

   , ns_geo AS
    (SELECT UPPER(iso.market10) AS market10
          , ns.platform_subset
          , MAX(ns.cal_date)    AS max_cal_date
     FROM prod.norm_shipments AS ns
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(ns.platform_subset)
              JOIN mdm.iso_country_code_xref AS iso
                   ON UPPER(iso.country_alpha2) = UPPER(ns.country_alpha2)
     WHERE ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND UPPER(hw.technology) IN ('INK')
     GROUP BY UPPER(iso.market10)
            , ns.platform_subset)

   , ns_pfs AS
    (SELECT ns.platform_subset
          , MAX(ns.cal_date) AS max_cal_date
     FROM prod.norm_shipments AS ns
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(ns.platform_subset)
     WHERE ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND UPPER(hw.technology) IN ('INK')
     GROUP BY ns.platform_subset)

   , test AS
    (SELECT DISTINCT iink.platform_subset
                   , iink.country_alpha2
                   , iink.split_name
                   , ns.max_cal_date  AS country_max_cal_date
                   , geo.max_cal_date AS geo_max_cal_date
                   , pfs.max_cal_date AS pfs_max_cal_date
     FROM iink
              LEFT JOIN ns_country AS ns
                        ON UPPER(ns.platform_subset) =
                           UPPER(iink.platform_subset)
                            AND
                           UPPER(ns.country_alpha2) = UPPER(iink.country_alpha2)
              LEFT JOIN ns_geo AS geo
                        ON UPPER(geo.platform_subset) =
                           UPPER(iink.platform_subset)
                            AND UPPER(geo.market10) = UPPER(iink.geography)
              LEFT JOIN ns_pfs AS pfs
                        ON UPPER(pfs.platform_subset) =
                           UPPER(iink.platform_subset)
     WHERE 1 = 1
       AND UPPER(iink.split_name) = 'I-INK')

SELECT CASE
           WHEN COALESCE(country_max_cal_date, geo_max_cal_date,
                         pfs_max_cal_date) < CAST('2022-10-01' AS date)
               THEN 'to-decay'
           WHEN COALESCE(country_max_cal_date, geo_max_cal_date,
                         pfs_max_cal_date) >= CAST('2022-10-01' AS date)
               THEN 'to-allocate-ltp'
           ELSE 'error' END AS test_column
     , COUNT(1)             AS row_count
FROM test
GROUP BY CASE
             WHEN COALESCE(country_max_cal_date, geo_max_cal_date,
                           pfs_max_cal_date) < CAST('2022-10-01' AS date)
                 THEN 'to-decay'
             WHEN COALESCE(country_max_cal_date, geo_max_cal_date,
                           pfs_max_cal_date) >= CAST('2022-10-01' AS date)
                 THEN 'to-allocate-ltp'
             ELSE 'error' END
ORDER BY 1
"""

# COMMAND ----------

compare2_df = read_redshift_to_df(configs) \
  .option("query", compare2) \
  .load()

compare2_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results
# MAGIC 
# MAGIC Some notes on iteration 2:
# MAGIC + 266 platform_subsets/country/geo combinations in iink not in norm_ships
# MAGIC + 7 platform_subsets

# COMMAND ----------

# MAGIC %md
# MAGIC ## IINK platform_subsets not in norm_ships

# COMMAND ----------

compare3 = """
WITH iink AS
    (SELECT month_begin
          , geography_grain
          , geography
          , country_alpha2
          , split_name
          , platform_subset
          , p2_attach
          , ib
     FROM "stage"."ib_03_iink_complete"
     WHERE 1 = 1
       AND CAST(month_begin AS DATE) > CAST('2022-10-01' AS DATE))

   , ns_pfs AS
    (SELECT ns.platform_subset
          , MAX(ns.cal_date) AS max_cal_date
     FROM prod.norm_shipments AS ns
              JOIN mdm.hardware_xref AS hw
                   ON UPPER(hw.platform_subset) = UPPER(ns.platform_subset)
     WHERE ns.version = (SELECT MAX(version) FROM prod.norm_shipments)
       AND UPPER(hw.technology) IN ('INK')
     GROUP BY ns.platform_subset)

   , test AS
    (SELECT DISTINCT iink.platform_subset
                   , iink.country_alpha2
                   , iink.split_name
                   , pfs.max_cal_date AS pfs_max_cal_date
     FROM iink
              LEFT JOIN ns_pfs AS pfs
                        ON UPPER(pfs.platform_subset) =
                           UPPER(iink.platform_subset)
     WHERE 1 = 1
       AND UPPER(iink.split_name) = 'I-INK')

SELECT DISTINCT platform_subset
FROM test
WHERE pfs_max_cal_date IS NULL
ORDER BY 1
"""

# COMMAND ----------

compare3_df = read_redshift_to_df(configs) \
  .option("query", compare3) \
  .load()

compare3_df.show()
