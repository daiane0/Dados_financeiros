from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime
import pandas as pd



senha_codificada = quote_plus('fcndq@9000')

url_conexao = f'postgresql://postgres:{senha_codificada}@localhost/finance_raw_data'

df = pd.read_csv('ifData_receita_lucro.csv', header=[0])

df = df.iloc[:1388, :40]


df.to_sql( 
    name = 'demonstracao_resultado_receita_lucro',
    con = url_conexao,
    index = False,
    if_exists = 'append')

