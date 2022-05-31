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
# MAGIC ## Compare IINK-STF to norm ships

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
# MAGIC There are no I-INK combinations (platform_subset, country_alpha2) in normalized shipments that extend past 10-01-2022. INCORRECT - I AM WORKING IN THE WRONG ENVIRONMENT!
