class RedshiftOut:
    def get_data(self, username, password, table_name, query):
        dataDF = spark.read \
            .format(spark_format) \
            .option("url", spark_url) \
            .option("tempdir", spark_temp_dir) \
            .option("aws_iam_role", aws_iam) \
            .option("user", username) \
            .option("password", password) \
            .option("query", query) \
            .load()

        return(dataDF)


    def save_table(self, dataDF):
        dataDF.write \
            .format(dbutils.spark_format) \
            .option("url", spark_url) \
            .option("dbtable", table_name) \
            .option("tempdir", spark_temp_dir) \
            .option("aws_iam_role", aws_iam) \
            .option("user", username) \
            .option("password", password) \
            .mode("overwrite") \
            .save()


  # from Matt Koson, Data Engineer
    def submit_remote_query(self, dbname, port, user, password, host, sql_query):
        conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
          .format(dbname, port, user, password, host)

        con = psycopg2.connect(conn_string)
        cur = con.cursor()
        cur.execute(sql_query)
        con.commit()
        cur.close()



for obj in query_list:
    table_name = obj[0]
    query = obj[1]
    query_name = table_name.split('.')[1]
    try:
        read_obj = RedshiftOut()
        data_df = read_obj.get_data(username, password, table_name, query)
        print("Query " + query_name + " retrieved.")
    except Exception(e):
        print("Error, query " + query_name + " not retrieved.")
        print(e)

    try:
        read_obj.save_table(data_df)
        read_obj.submit_remote_query("itg", "5439", username, password, redshift_url, f'GRANT ALL ON {table_name} TO group dev_arch_eng')
        print("Table " + table_name + " created.\n")
    except Exception(e):
        print("Error, table " + table_name + " not created.\n")
        print(e)

