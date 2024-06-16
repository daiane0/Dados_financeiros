import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

def connect_db():
    return psycopg2.connect(
        dbname="finance_raw_data",
        user="postgres",
        password="fcndq@9000",
        host="localhost",
        port="5432"
    )

#def engine():
    

#user = 'seu_usuario'
#password = 'sua_senha'
###database = 'seu_banco_de_dados'

# String de conexão
#connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

# Criar o motor de conexão
#engine = create_engine(connection_string)
