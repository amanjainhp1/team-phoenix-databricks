# Databricks notebook source
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from time import sleep
import json

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/splunk_logging_utils

# COMMAND ----------

job_data = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
splunk_data = log_job_start(app=job_data['tags']['jobName'], run_id=job_data['tags']['runId'])

# COMMAND ----------

notebooks = []

try:
    tables = json.loads(dbutils.widgets.get("tables"))
except:
    tables = json.loads("""
        {
            "adjusted_revenue_epa": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Financials",
                "source_schema": "dbo",
                "source_table": "adjusted_revenue_epa",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "fin_prod", 
                "destination_table": "adjusted_revenue_epa"
            },
            "actuals_supplies": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "actuals_supplies",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod",
                "destination_table": "actuals_supplies"
            },
            "calendar": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "calendar",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "mdm",
                "destination_table": "calendar"
            },
            "cartridge_demand_volumes": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "cartridge_demand_volumes",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod",
                "destination_table": "cartridge_demand_volumes"
            },
            "ce_splits": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "ce_splits",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "ce_splits"
            },
            "country_currency_map": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Staging",
                "source_schema": "dbo",
                "source_table": "country_currency_map_staging",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "country_currency_map"
            },
            "current_stf_dollarization": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials",
                "source_schema": "dbt", 
                "source_table": "current_stf_dollarization", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "fin_prod",
                "destination_table": "current_stf_dollarization"
            },
            "decay": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod", 
                "source_schema": "dbo", 
                "source_table": "decay",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod", 
                "destination_table": "decay"
            },
            "demand": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod", 
                "source_schema": "dbo", 
                "source_table": "demand",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod", 
                "destination_table": "demand"
            },
            "forecast_contra_input": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_contra_input",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_contra_input"
            },
            "forecast_fixed_cost_input": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_fixedcost_input",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_fixed_cost_input"
            },
            "forecast_gru_override": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_gru_override",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_gru_override"
            },
            "forecast_supplies_base_prod_region": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_supplies_baseprod_region",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_supplies_base_prod_region"
            },
            "forecast_supplies_base_prod_region_stf": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_supplies_baseprod_region_stf",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_supplies_base_prod_region_stf"
            },
            "forecast_variable_cost_ink": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_variablecost_ink",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_variable_cost_ink"
            },
            "forecast_variable_cost_toner": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials", 
                "source_schema": "dbo", 
                "source_table": "forecast_variablecost_toner",
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "fin_prod", 
                "destination_table": "forecast_variable_cost_toner"
            },
            "hardware_xref": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "hardware_xref", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "hardware_xref"
            },
            "hw_product_family_ink_forecaster_mapping": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "hw_product_family_ink_forecaster_mapping", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "hw_product_family_ink_forecaster_mapping"
            },
            "ibp_supplies_forecast": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "ibp_supplies_forecast", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "ibp_supplies_forecast"
            },
            "instant_ink_enrollees": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "instant_ink_enrollees", 
                "destination_system": "redshift",
                "destination_database": "",
                "destination_schema": "prod", 
                "destination_table": "instant_ink_enrollees"
            },
            "instant_ink_enrollees_ltf": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "instant_ink_enrollees_ltf", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "instant_ink_enrollees_ltf"
            },
            "iso_cc_rollup_xref": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "iso_cc_rollup_xref", 
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm", 
                "destination_table": "iso_cc_rollup_xref"
            },
            "iso_country_code_xref": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "iso_country_code_xref", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "iso_country_code_xref"
            },
            "list_price_eoq": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "list_price_eoq",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "list_price_eoq"
            },
            "list_price_eu_country_list": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "list_price_EU_CountryList",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "list_price_eu_country_list"
            },
            "list_price_term_codes": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Landing",
                "source_schema": "dbo",
                "source_table": "list_price_term_codes_landing",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "list_price_term_codes"
            },
            "list_price_term_codes": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Landing",
                "source_schema": "ms4",
                "source_table": "ms4_profit_center_hierarchy_staging",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "ms4_profit_center_hierarchy"
            },
            "npi_base_gru": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Financials",
                "source_schema": "dbo", 
                "source_table": "npi_base_gru", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "fin_prod", 
                "destination_table": "npi_base_gru"
            },
            "pl_toner_forecaster_mapping": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "pl_toner_forecaster_mapping", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "pl_toner_forecaster_mapping"
            },
            "planet_actuals": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Financials",
                "source_schema": "dbo", 
                "source_table": "planet_actuals", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "fin_prod", 
                "destination_table": "planet_actuals"
            },
            "printer_lag": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "printer_lag", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "mdm",
                "destination_table": "printer_lag"
            },
            "product_line_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "product_line_xref",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm", 
                "destination_table": "product_line_xref"
            },
            "product_line_scenarios_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "product_line_scenarios_xref",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm", 
                "destination_table": "product_line_scenarios_xref"
            },
            "profit_center_code_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo", 
                "source_table": "profit_center_code_xref", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "mdm",
                "destination_table": "profit_center_code_xref"
            },
            "stf_dollarization": {
                "source_system": "sqlserver",
                "source_database":"IE2_Financials",
                "source_schema": "dbo", 
                "source_table": "stf_dollarization", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "fin_prod",
                "destination_table": "stf_dollarization"
            },
            "supplies_finance_hier_restatements_2020_2021": {
                "source_system": "sqlserver",
                "source_database":"IE2_Landing",
                "source_schema": "dbo", 
                "source_table": "supplies_finance_hier_restatements_2020_2021", 
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "fin_prod",
                "destination_table": "supplies_finance_hier_restatements_2020_2021"
            },
            "supplies_hw_mapping": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "supplies_hw_mapping",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm",
                "destination_table": "supplies_hw_mapping"
            },
            "supplies_stf_landing": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Landing", 
                "source_schema": "dbo",
                "source_table": "supplies_stf_landing",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "stage",
                "destination_table": "supplies_stf_landing"
            },
            "supplies_xref": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "supplies_xref",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "mdm",
                "destination_table": "supplies_xref"
            },
            "trade_forecast": {
                "source_system": "sqlserver",
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "trade_forecast",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "prod",
                "destination_table": "trade_forecast"
            },
            "tri_printer_ref_landing": {
                "source_system": "sqlserver",
                "source_database":"IE2_Landing",
                "source_schema": "dbo",
                "source_table": "tri_printer_ref_landing",
                "destination_system": "redshift", 
                "destination_database": "",
                "destination_schema": "prod",
                "destination_table": "tri_printer_ref_landing"
            },
            "working_forecast_channel_fill": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "working_forecast_channel_fill",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "scen",
                "destination_table": "working_forecast_channel_fill"
            },
            "working_forecast_mix_rate": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "working_forecast_mix_rate",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "scen",
                "destination_table": "working_forecast_mix_rate"
            },
            "working_forecast_supplies_spares": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "working_forecast_supplies_spares",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "scen",
                "destination_table": "working_forecast_supplies_spares"
            },
            "working_forecast_usage_share": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "working_forecast_usage_share",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "scen",
                "destination_table": "working_forecast_usage_share"
            },
            "working_forecast_vtc_override": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "working_forecast_vtc_override",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "scen",
                "destination_table": "working_forecast_vtc_override"
            },
            "working_forecast_yield": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "working_forecast_yield",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "scen",
                "destination_table": "working_forecast_yield"
            },
            "yield": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod", 
                "source_schema": "dbo",
                "source_table": "yield",
                "destination_system": "redshift",
                "destination_database": "", 
                "destination_schema": "mdm",
                "destination_table": "yield"
            }
        }
    """)
    
date = datetime.today()

datestamp = date.strftime("%Y%m%d")
timestamp= str(int(date.timestamp()))

for table in tables:
    widgets = tables[table]

    widgets["datestamp"] = datestamp
    widgets["timestamp"] = timestamp

    widgets['load_large_tables'] = dbutils.widgets.get('load_large_tables')

    notebooks = notebooks + [["move_sfai_data_to_redshift", 0, widgets]]

# COMMAND ----------

successful_jobs = []
failed_jobs = []

def run_notebook(notebook: list) -> tuple:
    try:
        result = dbutils.notebook.run(notebook[0], timeout_seconds=notebook[1], arguments=notebook[2])
    except:
        result = "FAILED"
    sleep(1)
    return(notebook[2]["destination_table"], result)

with ThreadPoolExecutor(max_workers = 4) as executor:thread = executor.map(run_notebook, notebooks)

for result in thread:
    if "SUCCESS" in result:
        successful_jobs.append(result[0])
    elif "FAILED" in result:
        failed_jobs.append(result[0])

# COMMAND ----------

if len(failed_jobs) > 0:
    log_job_end(splunk_data, "FAILED")
    raise Exception("Job failed. " + str(failed_jobs) + " contains a FAILED status.")
else:
    log_job_end(splunk_data, "SUCCESS")
