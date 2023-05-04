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

# Datestamp
def datestamp_get():
    date = datetime.today()
    datestamp = date.strftime("%Y-%m-%d")
    return datestamp

# Retrieve credentials
def retrieve_credentials(env: str) -> dict[str, str]:
    # Establish a session to be used in subsequent call to AWS Secrets Manager
    sm_session = create_session(role_arn=constants['STS_IAM_ROLE'][env], set_env_vars = False)
    # Retrieve secret
    secret_val = secrets_get(secret_name=constants['REDSHIFT_SECRET_NAME'][env], session=sm_session)
    return secret_val

# Retrieve all table names in a given schema  
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

# Unload data to S3 from Redshift
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

# Unload data from prod
def redshift_data_sync(datestamp: str, table: str, credentials: dict[str, str]) -> None:
    schema_name = table.split(".")[0]
    table_name = table.split(".")[1]
    
    redshift_unload(dbname=configs["redshift_dbname"],
                    port=configs["redshift_port"],
                    user=configs["redshift_username"],
                    password=configs["redshift_password"],
                    host=configs["redshift_url"],
                    schema=schema_name,
                    table=table_name,
                    s3_url=f"{REDSHIFT_DATA_SYNC_BUCKET}/{datestamp}/{schema_name}/{table_name}/",
                    iam_role=f"{configs['aws_iam_role']},{constants['REDSHIFT_IAM_ROLE']['dev']}")

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
        submit_remote_query(constants['REDSHIFT_DATABASE'][destination_env],
                            constants['REDSHIFT_PORT'][destination_env],
                            credentials[destination_env]['username'],
                            credentials[destination_env]['password'],
                            constants['REDSHIFT_URL'][destination_env],
                            drop_table_query)

        # Copy data from ITG bucket to Redshift
        redshift_copy(dbname=constants['REDSHIFT_DATABASE'][destination_env],
                        port=constants['REDSHIFT_PORT'][destination_env],
                        user=credentials[destination_env]['username'],
                        password=credentials[destination_env]['password'],
                        host=constants['REDSHIFT_URL'][destination_env],
                        schema=schema_name,
                        table=table_name,
                        s3_url=f"{REDSHIFT_DATA_SYNC_BUCKET}/{datestamp}/{schema_name}/{table_name}/",
                        iam_role=constants['REDSHIFT_IAM_ROLE'][destination_env],
                        destination_env=destination_env)
    return None 

# COMMAND ----------

def main():
    # constants
    REDSHIFT_DATA_SYNC_BUCKET = f"{constants['S3_BASE_BUCKET']['itg']}redshift_data_sync".replace("s3a://", "s3://")
    
    # parameters
    # datestamp (YYYY-MM-DD)
    datestamp = datestamp_get()

    # Define destination envs
    destination_envs = ['itg', 'dev']
    
    # # Define tables to copy
    tables = []
    for schema in ['fin_prod', 'mdm', 'prod', 'scen']:
        tables += get_redshift_table_names(configs, schema)

    # Retrieve credentials for each env
    credentials = {env:retrieve_credentials(env) for env in destination_envs}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_table = {}
        for table in tables:
            future = executor.submit(redshift_data_sync, datestamp, table, credentials)
            future_to_table[future] = table
        
        for future in concurrent.futures.as_completed(future_to_table):
            table = future_to_table[future]
            result = future.result()

# COMMAND ----------

if __name__ == "__main__":
    main()
