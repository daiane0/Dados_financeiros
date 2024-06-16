from urllib.parse import quote_plus
import findspark

def table_to_df(database, table, spark):

    user = 'postgres'
    password = 'fcndq@9000'

    senha_codificada = quote_plus(password)

    jdbc_url = f'jdbc:postgresql://localhost:5432/{database}'
    
    options = {
            "url": jdbc_url,
            "dbtable": table,
            "user": 'postgres',
            "password": 'fcndq@9000',
            "driver": "org.postgresql.Driver"
        }

    df = spark.read.format("jdbc").options(**options).load()
    
    return df