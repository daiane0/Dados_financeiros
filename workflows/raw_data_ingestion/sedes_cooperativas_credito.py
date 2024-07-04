from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db

def collect_and_save_data():

    url = "https://olinda.bcb.gov.br/olinda/servico/Instituicoes_em_funcionamento/versao/v1/odata/SedesCooperativas?$top=10000&$format=json"

    requisicao = requests.get(url)
    info = requisicao.json()

    conexao = connect_db()

    curs = conexao.cursor()

    data_insert = []

    for dado in info['value']:
        data_insert.append((
                dado['CNPJ'], 
                dado['NOME_INSTITUICAO'], 
                dado['ENDERECO'], 
                dado['COMPLEMENTO'], 
                dado['BAIRRO'], 
                dado['CEP'], 
                dado['MUNICIPIO'], 
                dado['UF'], 
                dado['DDD'], 
                dado['TELEFONE'], 
                dado['CLASSE'], 
                dado['ASSOCIACAO'], 
                dado['CATEGCOOPSING'], 
                dado['FILIACAO'], 
                dado['E_MAIL'], 
                dado['SITIO_NA_INTERNET'], 
                dado['MUNICIPIO_IBGE']
            ))



    if data_insert:
        sql = "INSERT INTO cooperativas_credito (cnpj, nome, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, classe, criterio_associacao, categoria_cooperativa_singular, filiacao, email, sitio_internet, municipio_ibge) VALUES %s ON CONFLICT (cnpj, nome, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, classe, criterio_associacao, categoria_cooperativa_singular, filiacao, email, sitio_internet, municipio_ibge) DO NOTHING"


        execute_values(curs, sql, data_insert)

        conexao.commit()

    conexao.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sedes_cooperativas_credito',
    default_args=default_args,
    description='DAG para coleta e salvamento de dados',
    schedule_interval=timedelta(days=1)
)

collect_and_save_data_task = PythonOperator(
    task_id='collect_sedes_cooperativas',
    python_callable=collect_and_save_data,
    dag=dag
)

collect_and_save_data_task
