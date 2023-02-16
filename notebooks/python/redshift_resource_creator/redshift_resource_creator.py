# Databricks notebook source
# import python libraries
import json
import os
import re

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

dbutils.widgets.text("drop_tables", "") # false
dbutils.widgets.text("stack", "") # itg
dbutils.widgets.text("job_dbfs_path", "") # dbfs:/dataos-pipeline-springboard/itg/table-creator/green

# COMMAND ----------

# define method that parses table dict and constructs fields section of table create query
def set_fields(input_json_dict):
    fields = ""
    sort_keys_list = []

    for field in input_json_dict["fields"]:
        field_details = []

        field_details.append(field["name"])

        if field["type"] == "IDENTITY":
            field_details.append("""INTEGER NOT NULL DEFAULT "identity"(184279, 0, ('1,1'::character varying)::text)""")
        else:
            field_details.append(field["type"])

        if "encode" in field:
            field_details.append("ENCODE " + field["encode"])

        if list(input_json_dict["fields"])[0] == field:
            fields = str.join(" ", field_details)
        else:
            fields = str.join(",\n", (fields, str.join(" ", field_details)))

        if "sort_key" in field:
            if field["sort_key"] == True:
                sort_keys_list.append(field["name"])
                
    sort_keys = ""
    if len(sort_keys_list) > 0:
        sort_keys = "sortkey (" + str.join(",", sort_keys_list) + ")"
    sort_keys = sort_keys + ";"
    
    return(fields, sort_keys)

# COMMAND ----------

# define method that parses table dict and constructs permissions section of table create query
def set_permissions(input_json_dict, schema, table):
    permissions = ""

    if "permissions" in input_json_dict:
        for permission in input_json_dict["permissions"]:
            permission_details = []

            permission_details.append(permission["permission_type"])

            permission_details.append(permission["permission_type_level"])

            permission_details.append("on table " + schema + "." + table + " to")

            if permission["permission_target_level"] != "user":
                permission_details.append(permission["permission_target_level"])

            if permission["permission_target"] == "dev_arch_eng" and dbutils.widgets.get("stack") == 'prod':
                permission_details.append("phoenix_dev")
            else:
                permission_details.append(permission["permission_target"])

            permissions = permissions + str.join(" ", permission_details) + ";\n"
    return permissions

# COMMAND ----------

# define method to retrieve file list and add to list
def get_file_list(job_dbfs_subfolder, file_substring):
    root = dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + job_dbfs_subfolder
    input_files = []

    # add all json files to a list
    for path, subdirs, files in os.walk(root):
        for name in files:
            if file_substring in name:
                input_files.append(os.path.join(path, name))

    return input_files

# COMMAND ----------

# tables
# our "main" method
# for each json file, parse, construct table create statement, and submit to Redshift
input_json_files = get_file_list("/redshift/table_schema/", ".json")

for input_json_file in input_json_files:
    
    input_schema = input_json_file.split("/")[len(input_json_file.split("/"))-2]
    input_table_name = input_json_file.split("/")[len(input_json_file.split("/"))-1].replace(".json", "")
    
    input_json_dict = json.load(open(input_json_file))
    
    sql_query = "CREATE TABLE IF NOT EXISTS {}.{}\n(\n{}\n)\n\n{}\n\n{}".format(input_schema, input_table_name, set_fields(input_json_dict)[0], set_fields(input_json_dict)[1], set_permissions(input_json_dict, input_schema, input_table_name))
    
    drop_table_query = ""
    if dbutils.widgets.get("drop_tables").lower() == "true":
        drop_table_query = "DROP TABLE IF EXISTS {}.{};".format(input_schema, input_table_name)
        print("dropping table (if exists) {}.{}".format(input_schema, input_table_name))
    
    print("creating table {}.{}...".format(input_schema, input_table_name))
    submit_remote_query(configs, drop_table_query + sql_query)
    print("table {}.{} created!".format(input_schema, input_table_name))

# COMMAND ----------

# stored_procedures
# our "main" method
# for each json file, parse, construct table create statement, and submit to Redshift

input_files = get_file_list("/redshift/stored_procedures/", ".sql")

for input_file in input_files:
    
    sql_query = open(input_file).read()
    sproc_name = sql_query.split("\n")[0]
    sproc = re.sub('CREATE OR REPLACE PROCEDURE', '', sproc_name)
    
    permissions_query = f"""
    -- Permissions
    GRANT ALL ON PROCEDURE {sproc} TO {configs["redshift_username"]};
    GRANT ALL ON PROCEDURE {sproc} TO group {constants['REDSHIFT_DEV_GROUP'][dbutils.widgets.get("stack")]};
    """

    print("creating stored procedure {}...".format(sproc))
    submit_remote_query(configs, sql_query + "\n" + permissions_query)
    print("stored procedure {} created!".format(sproc))

# COMMAND ----------

# views
# our "main" method
# for each json file, parse, construct table create statement, and submit to Redshift
input_files = get_file_list("/redshift/views/", ".sql")

# retrieve relevant schemas in env/stack
rows = read_redshift_to_rows(configs, "SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema'")
schema_list = []
for row in rows:
    schema_list.append(row[0])

# retrieve datashare info
datashare_file = dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + "/redshift/datashares.json"
datashares = json.load(open(datashare_file))

for input_file in input_files:
    schema = input_file.split('/')[-2]
    if schema in schema_list:
        view_name = input_file.split('/')[-1].replace('.sql', '')
        view = f"{schema}.{view_name}"

        sql_query = open(input_file).read()
        permissions_query = f"""
        -- Permissions
        GRANT ALL ON TABLE {view} TO {configs["redshift_username"]};
        GRANT ALL ON TABLE {view} TO group {constants['REDSHIFT_DEV_GROUP'][dbutils.widgets.get("stack")]};
        """

        print("creating view {}...".format(view))
        submit_remote_query(configs, sql_query + "\n" + permissions_query)
        print("view {} created!".format(view))

        # add to relevant datashare if listed in datashares.json
        if stack in datashares:
            if schema in datashares[stack]:
                if view_name in datashares[stack][schema]:
                    submit_remote_query(configs, f"ALTER DATASHARE {schema} ADD {view};")
