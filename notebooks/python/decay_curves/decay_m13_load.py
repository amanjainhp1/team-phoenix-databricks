# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Decay m13 Load script

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# all notebook parameters
dbutils.widgets.text("append_decays", "")
dbutils.widgets.text("truncate_load_decays", "")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Append new decays

# COMMAND ----------

add_new_decay = """SELECT 'HW_DECAY' as record
		, d.platform_subset
	  , 'MARKET13' as geography_grain
	  , UPPER(market13) as geography
	  , concat('YEAR_',d.year) as year
	  , d.split_name
	  , d.value
	  , '' as avg_printer_life
	  , getdate() as load_date
	  , TO_CHAR(SYSDATE,'YYYY.MM.DD.1') as version,
	  , 1 as official
FROM stage.ink_toner_combined_decay_curves_final d
LEFT JOIN stage.decay_m13 m on m.platform_subset=d.platform_subset and m.geography=d.market13 and m.split_name=d.split_name and substring(m.year,6,2)=d.year
LEFT JOIN mdm.hardware_xref hw on hw.platform_subset=d.platform_subset
where hw.technology in ('INK','PWA','LASER') and m.platform_subset is null"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Truncate load decay

# COMMAND ----------

truncate_load_decay = 
"""SELECT 'HW_DECAY' as record
	, platform_subset
	, 'MARKET13' as geography_grain
	, UPPER(market13) as geography
	, CONCAT('YEAR_',year) as year
	, split_name
	, value
	, '' as avg_printer_life
	, SYSDATE as load_date
    , TO_CHAR(SYSDATE,'YYYY.MM.DD.1')
	, 1 as official
FROM stage.ink_toner_combined_decay_curves_final"""

# COMMAND ----------

if dbutils.widgets.get("append_decays").lower() == "true":
    query_list.append(["stage.decay_m13", add_new_decay, "append"])
    dbutils.notebook.run("../common/output_to_redshift",0, {"query_list": query_list})
if dbutils.widgets.get("truncate_load_decays").lower() == "true":
    query_list.append(["stage.decay_m13", truncate_load_decay, "overwrite"])
    dbutils.notebook.run("../common/output_to_redshift",0, {"query_list": query_list})
    

# COMMAND ----------


