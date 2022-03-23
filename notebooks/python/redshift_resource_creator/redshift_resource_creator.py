# Databricks notebook source
# import python libraries
import json
import os

# COMMAND ----------

# MAGIC %run ./common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ./common/database_utils

# COMMAND ----------

dbutils.widgets.text("redshift_secrets_name", "") # arn:aws:secretsmanager:us-west-2:740156627385:secret:itg/redshift/team-phoenix/auto_glue-v6JOfZ
dbutils.widgets.text("stack", "") # itg
dbutils.widgets.text("job_dbfs_path", "") # dbfs:/dataos-pipeline-springboard/itg/table-creator/green

# COMMAND ----------

# import constants
with open(dbutils.widgets.get("job_dbfs_path").replace("dbfs:", "/dbfs") + "/configs/constants.json") as json_file:
  constants = json.load(json_file)

# COMMAND ----------

# create configs
redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
 
configs = {}
configs["redshift_temp_bucket"] = "{}redshift_temp/".format(constants['S3_BASE_BUCKET'][dbutils.widgets.get("stack")])
configs["redshift_username"] = redshift_secrets["username"]
configs["redshift_password"] = redshift_secrets["password"]
configs["redshift_url"] = constants['REDSHIFT_URLS'][dbutils.widgets.get("stack")]
configs["redshift_port"] = constants['REDSHIFT_PORTS'][dbutils.widgets.get("stack")]
configs["redshift_dbname"] = dbutils.widgets.get("stack")

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
                
    sort_keys = "sortkey (" + str.join(",", sort_keys_list) + ");"
    
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
input_json_files = get_file_list("/table_schema/", ".json")

for input_json_file in input_json_files:
    
    input_schema = input_json_file.split("/")[len(input_json_file.split("/"))-2]
    input_table_name = input_json_file.split("/")[len(input_json_file.split("/"))-1].replace(".json", "")
    
    input_json_dict = json.load(open(input_json_file))
    
    sql_query = "CREATE TABLE IF NOT EXISTS {}.{}\n(\n{}\n)\n\n{}\n\n{}".format(input_schema, input_table_name, set_fields(input_json_dict)[0], set_fields(input_json_dict)[1], set_permissions(input_json_dict, input_schema, input_table_name))
    
    drop_table_query = ""
    if dbutils.widgets.get("drop_tables").lower() == "true":
        drop_table_query = "DROP TABLE IF EXISTS {}.{};".format(input_schema, input_table_name)
    
    submit_remote_query(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], drop_table_query + sql_query)

# COMMAND ----------

# stored_procedures
# our "main" method
# for each json file, parse, construct table create statement, and submit to Redshift

input_files = get_file_list("/stored_procedures/", ".sql")

for input_file in input_files:
    
    sql_query = open(input_file).read()
    
    permissions_query = """
    -- Permissions
    GRANT ALL ON PROCEDURE prod.addversion_sproc(varchar, varchar) TO {};
    GRANT ALL ON PROCEDURE prod.addversion_sproc(varchar, varchar) TO group {};
    """.format(configs["redshift_username"], constants['REDSHIFT_DEV_GROUP'])
    
    submit_remote_query(configs["redshift_dbname"], configs["redshift_port"], configs["redshift_username"], configs["redshift_password"], configs["redshift_url"], sql_query + "\n" + permissions_query)
