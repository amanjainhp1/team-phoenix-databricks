# Databricks notebook source
import uuid
from typing import List

# COMMAND ----------

# MAGIC %run ../config_forecasting_engine

# COMMAND ----------

# generate a uuid to uniquely identify the record set
guid = str(uuid.uuid4())

# COMMAND ----------

def update_table_version_info(
    configs: dict, version_info: Union[str, datetime], destination: str, dummy_version: str
) -> None:
    query = f"""
    UPDATE {destination}
    SET load_date = '{version_info[1]}',
    version = '{version_info[0]}'
    WHERE version = '{dummy_version}';
    """
    submit_remote_query(configs, query)
    return None


def read_stage_write_prod(input: List[str], technology_label: str, working_forecast_source_name: str):
    # read in result of query as DataFrame
    df = read_redshift_to_df(configs).option("query", input[1]).load()
    # write DataFrame out to prod.working_forecast table
    destination = f"prod.{input[0].lower()}"

    write_df_to_redshift(
        configs=configs, df=df, destination=destination, mode="append"
    )

    # add new version
    addversion_info = call_redshift_addversion_sproc(
        configs, f"{technology_label.upper()}-WORKING-FORECAST", f"{working_forecast_source_name}"
    )

    # update inserted records with new version info
    update_table_version_info(
        configs=configs,
        version_info=addversion_info,
        destination=destination,
        dummy_version=guid,
    )
    return addversion_info

# COMMAND ----------

working_forecast_query = f"""
SELECT 
    record
    , CAST(cal_date AS DATE) cal_date
    , geography_grain
    , geography
    , platform_subset
    , base_product_number
    , customer_engagement
    , cartridges
    , channel_fill
    , supplies_spares_cartridges
    ,{'0 AS' if technology == 'laser' else ''} host_cartridges --default to 0 for laser
    ,{'0 AS' if technology == 'laser' else ''} welcome_kits --default to 0 for laser
    , expected_cartridges
    , vtc
    , adjusted_cartridges
    , CAST(NULL AS DATE) load_date --create a dummy load_date which will be replaced after calling prod.addversion_sproc
    , CAST('{guid}' AS varchar(64)) version --create a dummy version which will be replaced after calling prod.addversion_sproc
FROM scen.{technology_label}_working_fcst
"""

read_stage_write_prod(input=["working_forecast", working_forecast_query], technology_label=technology_label, working_forecast_source_name=working_forecast_source_name)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="working_forecast_version", value=addversion_info[1])
