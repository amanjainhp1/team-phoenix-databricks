# Databricks notebook source
import boto3
import concurrent.futures
import json
import psycopg2
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./common/configs

# COMMAND ----------

# MAGIC %run ./common/database_utils

# COMMAND ----------

# constants
REDSHIFT_DATA_SYNC_BUCKET = f"{constants['S3_BASE_BUCKET']['itg']}redshift_data_sync".replace("s3a://", "s3://")

# COMMAND ----------

# Datestamp
def datestamp_get():
    date = datetime.today()
    datestamp = date.strftime("%Y-%m-%d")
    return datestamp

# function to retrieve all table names in a given schema  
def get_redshift_table_names(configs:dict, schema: str):
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'])
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    cur.execute(f" SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema='{schema}' AND table_type <> 'VIEW'")
    data = cur.fetchall()
    table_list = [row[0] for row in data]
    cur.close()
    return(table_list)

# Define function to unload data to S3 from Redshift
def redshift_unload(dbname: str, port: str, user: str, password: str, host: str, schema: str, table: str, s3_url: str, iam_role: str):
    print(f'prod|Started unloading {schema}.{table}')
    start_time = time.time()
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(dbname, port, user, password, host)
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{table}' AND (column_default NOT LIKE '%identity%' OR column_default IS NULL) ORDER BY ordinal_position asc")
    data = cur.fetchall()
    col_list = [row[0] for row in data]
    select_statement = "SELECT " + ", ".join(col_list) + f" FROM {schema}.{table}"
    cur.close()

    unload_query = f"UNLOAD ('{select_statement}') TO '{s3_url}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET ALLOWOVERWRITE;"
    submit_remote_query(dbname, port, user, password, host, unload_query)
    function_duration = round(time.time()-start_time, 2)
    print(f'prod|Finished unloading {schema}.{table} in {function_duration}s')

# Retrieve ddl
def redshift_retrieve_ddl(dbname: str, port: str, user: str, password: str, host: str, schema: str, table: str):
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(dbname, port, user, password, host)
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    cur.execute(f"SELECT ddl FROM prod.generate_tbl_ddl_vw WHERE schemaname='{schema}' AND tablename='{table}'")
    data = cur.fetchall()
    ddl = ""
    for row in data:
        ddl += ("\n" + row[0])
    return ddl

# Rebuild table and copy data from S3 to Redshift
def redshift_copy(dbname:str, port: str, user: str, password: str, host:str, schema: str, table: str, s3_url: str, iam_role: str, destination_env:str):
    print(f'{destination_env}|Started copying {schema}.{table}')
    start_time = time.time()
    copy_query = f"COPY {schema}.{table} FROM '{s3_url}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET;"
    # update permissions
    permissions_query = f"GRANT ALL ON {schema}.{table} TO GROUP dev_arch_eng;"
    submit_remote_query(dbname, port, user, password, host, copy_query + permissions_query)
    function_duration = round(time.time()-start_time, 2)
    print(f'{destination_env}|Finished copying {schema}.{table} in {function_duration}s')

# COMMAND ----------

# input parameters
# destination envs (itg, dev)
try:
    destination_envs = list(dbutils.widgets.get("destination_envs"))
except:
    destination_envs = ['itg', 'dev']

# tables to unload/copy
try:
    tables = list(dbutils.widgets.get("tables"))
except:
    tables = []
    for schema in ['fin_prod', 'mdm', 'prod', 'scen']:
        tables += get_redshift_table_names(configs, schema)

# COMMAND ----------

# retrieve credentials (need to be refreshed monthly)
credentials = {}
for destination_env in destination_envs:
    credentials[destination_env] = json.loads(dbutils.secrets.get(scope='team-phoenix', key=f'redshift-{destination_env}'))

# COMMAND ----------

# datestamp (YYYY-MM-DD)
datestamp = datestamp_get()

# COMMAND ----------

# unload data from prod
def redshift_data_sync(table: str):
    schema = table.split(".")[0]
    table = table.split(".")[1]
    
    redshift_unload(dbname=configs["redshift_dbname"],
                    port=configs["redshift_port"],
                    user=configs["redshift_username"],
                    password=configs["redshift_password"],
                    host=configs["redshift_url"],
                    schema=schema,
                    table=table,
                    s3_url=f"{REDSHIFT_DATA_SYNC_BUCKET}/{datestamp}/{schema}/{table}/",
                    iam_role=f"{configs['aws_iam_role']},{constants['REDSHIFT_IAM_ROLE']['dev']}")

    # build query to drop and rebuild table in lower environment/s
    ddl = redshift_retrieve_ddl(dbname=configs["redshift_dbname"],
                                port=configs["redshift_port"],
                                user=configs["redshift_username"],
                                password=configs["redshift_password"],
                                host=configs["redshift_url"],
                                schema=schema,
                                table=table)
    drop_table_query = f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;\n" + ddl
    
    # copy data to itg/dev
    for destination_env in destination_envs:
        submit_remote_query(constants['REDSHIFT_DATABASE'][destination_env],
                            constants['REDSHIFT_PORT'][destination_env],
                            credentials[destination_env]['username'],
                            credentials[destination_env]['password'],
                            constants['REDSHIFT_URL'][destination_env],
                            drop_table_query)

        # copy data from ITG bucket to Redshift
        redshift_copy(dbname=constants['REDSHIFT_DATABASE'][destination_env],
                        port=constants['REDSHIFT_PORT'][destination_env],
                        user=credentials[destination_env]['username'],
                        password=credentials[destination_env]['password'],
                        host=constants['REDSHIFT_URL'][destination_env],
                        schema=schema,
                        table=table,
                        s3_url=f"{REDSHIFT_DATA_SYNC_BUCKET}/{datestamp}/{schema}/{table}/",
                        iam_role=constants['REDSHIFT_IAM_ROLE'][destination_env],
                        destination_env=destination_env)

# COMMAND ----------

# start worker threads with maximum of 2 concurrent threads
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    results = [executor.submit(redshift_data_sync, table) for table in tables]
