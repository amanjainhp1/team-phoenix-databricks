# Databricks notebook source
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from time import sleep
import json

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

notebooks = []

try:
    tables = json.loads(dbutils.widgets.get("tables"))
except:
    tables = json.loads("""
        {
            "acct_rates": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "acct_rates",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "prod", 
                "destination_table": "acct_rates"
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
                "source_database":"IE2_Landing",
                "source_schema": "dbo",
                "source_table": "country_currency_map_landing",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "country_currency_map"
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
            "list_price_eu_countrylist": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Prod",
                "source_schema": "dbo",
                "source_table": "list_price_EU_CountryList",
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "mdm", 
                "destination_table": "list_price_eu_countrylist"
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
            "odw_document_currency": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Financials",
                "source_schema": "ms4", 
                "source_table": "odw_document_currency", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "fin_prod", 
                "destination_table": "odw_document_currency"
            },
            "odw_sacp_actuals": {
                "source_system": "sqlserver", 
                "source_database":"IE2_Financials",
                "source_schema": "ms4", 
                "source_table": "odw_sacp_actuals", 
                "destination_system": "redshift", 
                "destination_database": "", 
                "destination_schema": "fin_prod", 
                "destination_table": "odw_sacp_actuals"
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
    raise Exception("Job failed. " + str(failed_jobs) + " contains a FAILED status.")
