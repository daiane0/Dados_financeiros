from urllib.parse import quote_plus
from pyspark.sql.types import *
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DateType, TimestampType, StructType, StructField
import re
    

def table_to_df(db, table, sparkSession):
    # Create engine if db is a connection string
    if isinstance(db, str):
        # Adjust the URL to include the database name at the end
        user = 'postgres'
        password = 'fcndq@9000'
        senha_codificada = quote_plus(password)
        url = f'postgresql://{user}:{senha_codificada}@localhost:5432/{db}'
        engine = create_engine(url)
    else:
        engine = db  # Assume db is already an Engine object

    try:
        # Reflect the table metadata
        meta = MetaData(bind=engine)
        meta.reflect(engine, only=[table])

        # Get column names and types
        column_names = meta.tables[table].columns.keys()
        column_types = [col.type for col in meta.tables[table].columns]

        # Map SQLAlchemy types to PySpark types
        type_map = {
            "INTEGER": IntegerType(),
            "FLOAT": FloatType(),
            "DOUBLE": DoubleType(),
            "VARCHAR": StringType(),
            "TEXT": StringType(),
            "BOOLEAN": BooleanType(),
            "DATE": DateType(),
            "TIMESTAMP": TimestampType(),
            "CHARACTER VARYING": StringType(),
            "VARCHAR(255)": StringType()
            # Add more types as needed
        }

        # Define the schema
        schema = StructType([StructField(column, type_map[str(dtype)], True) for column, dtype in zip(column_names, column_types)])

        # Fetch data from the table
        query = f"SELECT * FROM {table}"
        data = engine.execute(query)

        # Convert to a list of Rows (Spark SQL)
        rows = [Row(*row) for row in data.fetchall()]

        # Create Spark DataFrame
        df = sparkSession.createDataFrame(rows, schema=schema)

        # Optionally show the DataFrame or return it

        return df

    except SQLAlchemyError as e:
        print(f"Error connecting to database or querying table: {e}")
        return None

        
def url_to_connect(database):

    user = 'postgres'
    
    password = 'fcndq@9000'

    senha_codificada = quote_plus(password)

    url = f'postgresql://{user}:{senha_codificada}@localhost:5432/{database}'
    
    return url