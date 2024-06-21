
def get_last_load_date(table_name, data_type, sparkSession):
    sql_query = f"""
        SELECT MAX(load_date) AS max_load_date
          FROM dw_load_control
         WHERE table_name = '{table_name}'
           AND data_type = '{data_type}'
    """


    result = spark.sql(sql_query)
    max_load_date = result.collect()[0]['max_load_date']
    return max_load_date

def update_last_load_date(data, table_name, data_type)
    sql_update_dw_load_control = f"""
        INSERT INTO dw_load_control (load_date, table_name, data_type)
        VALUES ('{data}', '{table_name}', '{data_type}');
    """
