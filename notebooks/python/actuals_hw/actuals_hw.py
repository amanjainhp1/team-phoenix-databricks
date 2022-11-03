# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ####Populate Initial Redshift Actuals Hardware Table

# COMMAND ----------

from datetime import date

# Check if destination table has row count > 0, if not, then copy full source table to destination
source_database = ""
source_schema = ""
source_table = ""

destination_schema = "prod"
destination_table = "actuals_hw"

query = ""

initial_data_load = False
destination_table_exists = False

datestamp = date.today().strftime("%Y%m%d")

try:
    query = f"""SELECT COUNT(*) FROM {destination_schema}.{destination_table}"""

    destination_table_row_count = read_redshift_to_df(configs) \
        .option("query", query) \
        .load() \
        .head()[0]

    #if destination table exists, continue else will hit catch block
    destination_table_exists = True

    initial_data_load = True if destination_table_row_count == 0 else False

    if initial_data_load:
        source_database = "IE2_Prod"
        source_schema = "dbo"
        source_table = "actuals_hw"

        source_table_df = read_sql_server_to_df(configs) \
            .option("dbTable", f"""{source_database}.{source_schema}.{source_table}""") \
            .load()

        #  save "landing" data to S3
        write_df_to_s3(source_table_df, constants["S3_BASE_BUCKET"][stack] + f"/product/{destination_table}/{datestamp}/", "csv", "overwrite")

        source_table_df = source_table_df.select("record", "cal_date", "country_alpha2", "base_product_number", "platform_subset", "source", "base_quantity", "official", "load_date", "version")

        #  save data to "stage" and final/"prod" schema
        write_df_to_redshift(configs, source_table_df, f"{destination_schema}.{destination_table}", "append")

except Exception as error:
    print("An exception has occured:", error)
    print("Exception Type:", type(error))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Redshift Actuals Hardware Table - Staging

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from pyspark.sql import Row 
from pyspark.sql.functions import col, lit, when

append_to_prod_table = False

if initial_data_load == False and destination_table_exists:
# check the max date from archer and compare to max cal_date in prod table

    query = f"""
    SELECT MAX(date) AS archer_date
    FROM [Archer_Prod].dbo.stf_flash_country_speedlic_yeti_vw
    WHERE record = 'Planet-Actuals'
    """

    archer_actuals_units_max_date = read_sql_server_to_df(configs) \
        .option("query", query) \
        .load() \
        .select("archer_date").head()[0]

    query = f"""
    SELECT MAX(cal_date) AS phoenix_date
    FROM prod.actuals_hw
    WHERE 1=1 AND source = 'ARCHER'
    """

    redshift_actuals_units_max_date = read_redshift_to_df(configs) \
    .option("query", query) \
    .load() \
    .select("phoenix_date").head()[0]


    # if archer has newer data
    #    * build stage dataframe
    #    * run stored proc to create new version
    #    * retrieve new version and load_date
    #    * modify version and load_date values in stage dataframe
    #    * write stage table

    append_to_prod_table = True if archer_actuals_units_max_date > redshift_actuals_units_max_date else False

    if append_to_prod_table:

        # create empty data frame 
        actuals_schema = StructType([ \
            StructField("record", StringType(), True), \
            StructField("cal_date", DateType(), True), \
            StructField("country_alpha2", StringType(), True), \
            StructField("base_product_number", StringType(), True), \
            StructField("Platform_Subset", StringType(), True), \
            StructField("base_quantity", DecimalType(38,2), True), \
            StructField("load_date", TimestampType(), True), \
            StructField("official", IntegerType(), True), \
            StructField("version", IntegerType(), True), \
            StructField("source", StringType(), True) \
        ])

        stage_actuals_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), actuals_schema)

        #retrieve relevant data from source db
        planet_actuals = read_sql_server_to_df(configs) \
            .option("query", f"""SELECT * FROM [Archer_Prod].dbo.stf_flash_country_speedlic_yeti_vw WHERE record = 'Planet-Actuals' AND date = '{archer_actuals_units_max_date}'""") \
            .load()
        
        # store data, in raw format from source DB to stage table
        write_df_to_redshift(configs, planet_actuals, "stage.planet_actuals", "overwrite")

        staging_actuals_units_hw_query = """
        SELECT
          'ACTUALS - HW' AS record,
          a.date AS cal_date,
          a.geo AS country_alpha2,
          a.base_prod_number AS base_product_number,
          b.platform_subset,
          sum(a.units) AS base_quantity,
          a.load_date,
          1 AS official,
          a.version,
          'ARCHER' AS source
        FROM stage.planet_actuals a
        LEFT JOIN mdm.rdma b
          ON UPPER(a.base_prod_number) = UPPER(b.base_prod_number)
        WHERE a.units <> 0
        GROUP BY
          a.date,
          a.geo,
          a.base_prod_number,
          b.platform_subset,
          a.load_date,
          a.version
        """

        staging_actuals_units_hw = read_redshift_to_df(configs) \
            .option("query", staging_actuals_units_hw_query) \
            .load()

        #   save "landing" data to S3
        write_df_to_s3(staging_actuals_units_hw, constants["S3_BASE_BUCKET"][stack] + f"/product/{destination_table}/sf/{datestamp}/", "parquet", "overwrite")

        #execute stored procedure to create new version and load date
        max_version_info = call_redshift_addversion_sproc(configs, 'ACTUALS - HW', 'ARCHER')

        max_version = max_version_info[0]
        max_load_date = max_version_info[1]

        staging_actuals_units_hw = staging_actuals_units_hw \
            .withColumn("version", when(col("version") != (max_version), max_version)) \
            .withColumn("load_date", when(col("load_date") != (max_load_date), max_load_date))

        stage_actuals_df = stage_actuals_df.union(staging_actuals_units_hw)

        #retrieve large format SFAI data
        odw_revenue_units_base_landing_query = """
        SELECT
            cal_date,
            country_alpha2,
            base_product_number,
            base_quantity
        FROM fin_prod.odw_revenue_units_base_actuals
        WHERE 1=1
            and cal_date = (SELECT MAX(cal_date) FROM fin_prod.odw_revenue_units_base_actuals)
            AND base_quantity <> 0
        """
        
        odw_revenue_units_base_landing = read_redshift_to_df(configs) \
            .option("query", odw_revenue_units_base_landing_query) \
            .load()

        write_df_to_redshift(configs, odw_revenue_units_base_landing, "stage.odw_revenue_units_base_landing", "overwrite")

        #join EDW data to lookup tables in Redshift
        staging_actuals_lf_query = f"""
            SELECT
              'ACTUALS_LF' AS record,
              e.cal_date,
              e.country_alpha2,
              e.base_product_number,
              r.Platform_Subset,
              SUM(e.base_quantity) AS base_quantity,
              NULL AS load_date,
              1 AS official,
              NULL AS version,
              'ODW-LF' AS source
            FROM stage.odw_revenue_units_base_landing e
            LEFT JOIN mdm.rdma r on r.Base_Prod_Number = e.base_product_number
            LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = r.Platform_Subset
            WHERE 1=1
                AND hw.technology = 'LF'
            GROUP BY
            e.cal_date,
            e.country_alpha2,
            e.base_product_number,
            r.Platform_Subset
        """

        staging_actuals_lf = read_redshift_to_df(configs) \
            .option("query", staging_actuals_lf_query) \
            .load()

        staging_actuals_lf = staging_actuals_lf.withColumn("load_date", col("load_date").cast("timestamp"))
    
        if staging_actuals_lf.count() > 0:
            #   save "landing" data to S3
            write_df_to_s3(staging_actuals_lf, constants["S3_BASE_BUCKET"][dbutils.widgets.get("stack")] + f"/product/{destination_table}/lf/{datestamp}/", "parquet", "overwrite")

            max_version_info = call_redshift_addversion_sproc(configs, 'ACTUALS_LF', 'ODW-LF')

            max_version = max_version_info[0]
            max_load_date = max_version_info[1]

            staging_actuals_lf = staging_actuals_lf \
                .withColumn("version", lit(max_version)) \
                .withColumn("load_date", lit(max_load_date))

            stage_actuals_df = stage_actuals_df.union(staging_actuals_lf)

        if stage_actuals_df.count() > 0:
            #write to redshift
            write_df_to_redshift(configs, stage_actuals_df, "stage.actuals_hw", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Redshift Actuals Hardware Table - Prod

# COMMAND ----------

# create final dataframe and write out to table

if initial_data_load == False and append_to_prod_table and destination_table_exists:

    query = f"""
    SELECT
        record,
        cal_date,
        country_alpha2,
        base_product_number,
        a.platform_subset,
        source,
        base_quantity,
        a.official,
        a.load_date,
        a.version
    FROM stage.actuals_hw a
    LEFT JOIN mdm.hardware_xref b ON a.platform_subset = b.platform_subset
    LEFT JOIN mdm.product_line_xref c ON b.pl = c.pl
    WHERE c.PL_category = 'HW'
        AND c.Technology IN ('INK','LASER','PWA','LF')
        AND a.base_quantity <> 0
    """

    redshift_stage_actuals_hw = read_redshift_to_df(configs) \
        .option("query", query) \
        .load()

    if append_to_prod_table:
        write_df_to_redshift(configs, redshift_stage_actuals_hw, f"""{destination_schema}.{destination_table}""", "append")
        
        # push the latest month of data back into SFAI
        sqlserver_actuals_hw = redshift_stage_actuals_hw \
            .select('record', 'cal_date', 'country_alpha2', 'base_product_number', 'platform_subset',
                    'base_quantity', 'load_date', 'official', 'version', 'source') \
            .filter("record = 'ACTUALS - HW'") \
            .orderBy('cal_date')
            
        write_df_to_sqlserver(configs=configs, df=sqlserver_actuals_hw, destination="IE2_Prod.dbo.actuals_hw", mode="append")
