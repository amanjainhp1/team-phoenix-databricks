# Databricks notebook source
import boto3
import concurrent.futures
import json
import psycopg2
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/datetime_utils

# COMMAND ----------

# create empty widgets for interactive sessions
dbutils.widgets.text('destination_envs', '')
dbutils.widgets.text('tables', '')
dbutils.widgets.text('backup_only', '')

# COMMAND ----------

# Retrieve credentials
def retrieve_credentials(env: str) -> dict[str, str]:
    try:
        # Establish a session to be used in subsequent call to AWS Secrets Manager
        sm_session = create_session(role_arn=constants['STS_IAM_ROLE'][env], set_env_vars = False)
        # Retrieve secret
        secret_val = secrets_get(secret_name=constants['REDSHIFT_SECRET_NAME'][env], session=sm_session)
        return secret_val
    except Exception as error:
        print (f"{env.lower()}|An exception has occured while attempting to retrieve Redshift credentials:", error)
        print (f"{env.lower()}|Exception Type:", type(error))
        raise Exception(error)

# Retrieve all table names in a given schema  
def get_redshift_table_names(configs:dict, schema: str):
    try:
        conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
            .format(configs['redshift_dbname'], configs['redshift_port'], configs['redshift_username'], configs['redshift_password'], configs['redshift_url'])
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
        cur.execute(f" SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema='{schema}' AND table_type <> 'VIEW'")
        data = cur.fetchall()
        table_list = [row[0] for row in data]
        cur.close()
        return(table_list)
    except Exception as error:
        print (f"{stack.lower()}|An exception has occured while attempting to retrieve tables in {schema} schema:", error)
        print (f"{stack.lower()}|Exception Type:", type(error))
        raise Exception(error)

# Unload data to S3 from Redshift
def redshift_unload(dbname: str, port: str, user: str, password: str, host: str, schema: str, table: str, s3_url: str, iam_role: str):
    print(f'prod|Started unloading {schema}.{table} to {s3_url}')
    start_time = time.time()
    select_statement = ""
    try:
        conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
            .format(dbname, port, user, password, host)
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
        # Retrieve all columns except those with identity type
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{table}' AND (column_default NOT LIKE '%identity%' OR column_default IS NULL) ORDER BY ordinal_position asc")
        data = cur.fetchall()
        if len(data) == 0:
            raise Exception(f"Unable to retrieve table information for {schema}.{table}. Check DDL.")
        col_list = [row[0] for row in data]
        select_statement = "SELECT " + ", ".join(col_list) + f" FROM {schema}.{table}"
        cur.close()
    except Exception as error:
        print (f"prod|An exception has occured while attempting to retrieve column names of {schema}.{table}:", error)
        print (f"prod|Exception Type:", type(error))
        raise Exception(error)

    try:
        unload_query = f"UNLOAD ('{select_statement}') TO '{s3_url}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET ALLOWOVERWRITE;"
        submit_remote_query(dbname, port, user, password, host, unload_query)
        function_duration = round(time.time()-start_time, 2)
        print(f'prod|Finished unloading {schema}.{table} in {function_duration}s')
    except Exception as error:
        print (f"prod|An exception has occured while attempting to unload {schema}.{table}:", error)
        print (f"prod|Exception Type:", type(error))
        raise Exception(error)

# Retrieve ddl
def redshift_retrieve_ddl(dbname: str, port: str, user: str, password: str, host: str, schema: str, table: str):
    try:
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
    except Exception as error:
        print (f"prod|An exception has occured while attempting to retrieve ddl of {schema}.{table}:", error)
        print (f"prod|Exception Type:", type(error))
        raise Exception(error)

# Rebuild table and copy data from S3 to Redshift
def redshift_copy(dbname:str, port: str, user: str, password: str, host:str, schema: str, table: str, s3_url: str, iam_role: str, destination_env:str):
    print(f'{destination_env}|Started copying {schema}.{table}')
    start_time = time.time()
    copy_query = f"COPY {schema}.{table} FROM '{s3_url}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET;"
    # Update permissions
    permissions_query = f"GRANT ALL ON {schema}.{table} TO GROUP dev_arch_eng;"
    try:
        submit_remote_query(dbname, port, user, password, host, copy_query + permissions_query)
    except Exception as error:
        print (f"{destination_env}|An exception has occured while attempting to copy {schema}.{table}:", error)
        print (f"{destination_env}|Exception Type:", type(error))
        raise Exception(error)
    function_duration = round(time.time()-start_time, 2)
    print(f'{destination_env}|Finished copying {schema}.{table} in {function_duration}s')

# Replace "invalid" characters for each element in a list of strings
def replace_invalid_characters(list_of_strings: list, string_pattern: str, string_replacement: str = '') -> list:
    pattern = re.compile(string_pattern)
    return [re.sub(pattern, string_replacement, element) for element in list_of_strings]

# COMMAND ----------

# Unload data from prod
def redshift_data_sync(datestamp: str, timestamp:str, table: str, credentials: dict[str, str], destination_envs: str, backup_only: bool) -> None:
    if backup_only:
        REDSHIFT_DATA_SYNC_BUCKET = f"{constants['S3_BASE_BUCKET'][stack]}redshift_data_sync".replace("s3a://", "s3://")
        iam_role=f"{configs['aws_iam_role']}"
    else:
        # Unload location for both dev and itg is itg S3 bucket 
        REDSHIFT_DATA_SYNC_BUCKET = f"{constants['S3_BASE_BUCKET']['itg']}redshift_data_sync".replace("s3a://", "s3://")
        iam_role = f"{configs['aws_iam_role']},{constants['REDSHIFT_IAM_ROLE']['dev']}"

    schema_name = table.split(".")[0]
    table_name = table.split(".")[1]


    redshift_unload(dbname=configs["redshift_dbname"],
                    port=configs["redshift_port"],
                    user=configs["redshift_username"],
                    password=configs["redshift_password"],
                    host=configs["redshift_url"],
                    schema=schema_name,
                    table=table_name,
                    s3_url=f"{REDSHIFT_DATA_SYNC_BUCKET}/{datestamp}/{timestamp}/{schema_name}/{table_name}/",
                    iam_role=iam_role)

    if not backup_only:
        # Build query to drop and rebuild table in lower environment/s
        ddl = redshift_retrieve_ddl(dbname=configs["redshift_dbname"],
                                    port=configs["redshift_port"],
                                    user=configs["redshift_username"],
                                    password=configs["redshift_password"],
                                    host=configs["redshift_url"],
                                    schema=schema_name,
                                    table=table_name)
        drop_table_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;\n" + ddl
        
        # Copy data to itg/dev
        for destination_env in destination_envs:
            try:
                submit_remote_query(constants['REDSHIFT_DATABASE'][destination_env],
                                    constants['REDSHIFT_PORT'][destination_env],
                                    credentials[destination_env]['username'],
                                    credentials[destination_env]['password'],
                                    constants['REDSHIFT_URL'][destination_env],
                                    drop_table_query)
            except Exception as error:
                print (f"{destination_env}|An exception has occured while attempting to drop/create {table}:", error)
                print (f"{destination_env}|Exception Type:", type(error))
                raise Exception(error)

            # Copy data from ITG bucket to Redshift
            redshift_copy(dbname=constants['REDSHIFT_DATABASE'][destination_env],
                            port=constants['REDSHIFT_PORT'][destination_env],
                            user=credentials[destination_env]['username'],
                            password=credentials[destination_env]['password'],
                            host=constants['REDSHIFT_URL'][destination_env],
                            schema=schema_name,
                            table=table_name,
                            s3_url=f"{REDSHIFT_DATA_SYNC_BUCKET}/{datestamp}/{timestamp}/{schema_name}/{table_name}/",
                            iam_role=constants['REDSHIFT_IAM_ROLE'][destination_env],
                            destination_env=destination_env)
    return None

# COMMAND ----------

def main():
    # parameters
    date = Date()
    # datestamp (YYYY-MM-DD)
    datestamp = date.getDatestamp("%Y-%m-%d")
    # UNIX timestamp
    timestamp = date.getTimestamp()

    # parse backup_only parameter
    backup_only = True if dbutils.widgets.get('backup_only').lower().strip() == 'true' else False

    # Define destination envs
    destination_envs = ['itg', 'dev'] if dbutils.widgets.get('destination_envs') == '' else dbutils.widgets.get('destination_envs').lower().split(',')
    destination_envs = replace_invalid_characters(list_of_strings=destination_envs, string_pattern='[^a-zA-Z]+')
    
    # Define tables to copy
    tables = []
    if dbutils.widgets.get('tables') == '':
        for schema in ['fin_prod', 'mdm', 'prod', 'scen']:
            tables += get_redshift_table_names(configs, schema)
    elif "." not in dbutils.widgets.get('tables'):
        schemas = dbutils.widgets.get('tables') \
            .lower() \
            .split(',')
        schemas = replace_invalid_characters(list_of_strings=schemas, string_pattern='[^0-9a-zA-Z]+')
        for schema in schemas:
            tables += get_redshift_table_names(configs, schema)
    else:
        tables = dbutils.widgets.get('tables') \
            .lower() \
            .split(',')
        tables = replace_invalid_characters(list_of_strings=tables, string_pattern='[^0-9a-zA-Z._]+')

    # Retrieve credentials for each env
    credentials = {env:retrieve_credentials(env) for env in destination_envs}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_table = {}
        for table in tables:
            future = executor.submit(redshift_data_sync, datestamp, timestamp, table, credentials, destination_envs, backup_only)
            future_to_table[future] = table
        
        for future in concurrent.futures.as_completed(future_to_table):
            table = future_to_table[future]
            result = future.result()

# COMMAND ----------

if __name__ == "__main__":
    main()
