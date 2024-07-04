from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db

def collect_bancos():

    url = "https://olinda.bcb.gov.br/olinda/servico/Instituicoes_em_funcionamento/versao/v1/odata/SedesBancoComMultCE?$top=100000&$format=json"

    requisicao = requests.get(url)
    info = requisicao.json()


    conexao = connect_db()

    curs = conexao.cursor()

    data_insert = []
    
    for dado in info['value']:
        data_insert.append((
                dado['CNPJ'],
                dado['NOME_INSTITUICAO'],
                dado['SEGMENTO'],
                dado['ENDERECO'],
                dado['COMPLEMENTO'],
                dado['BAIRRO'],
                dado['CEP'],
                dado['MUNICIPIO'],
                dado['UF'],
                dado['DDD'],
                dado['TELEFONE'], 
                dado['CARTEIRA_COMERCIAL'], 
                dado['E_MAIL'], 
                dado['SITIO_NA_INTERNET'], 
                dado['MUNICIPIO_IBGE']
            ))


    if data_insert:

        sql = "INSERT INTO bancos (cnpj, nome_instituicao, segmento, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, carteira_comercial, email, sitio_internet, municipio_ibge) VALUES %s ON CONFLICT (cnpj, nome_instituicao, segmento, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, carteira_comercial, email, sitio_internet, municipio_ibge) DO NOTHING"

        execute_values(curs, sql, data_insert)

        conexao.commit()

    conexao.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Sedes_bancos',
    default_args=default_args,
    description='DAG para coleta e salvamento de dados',
    schedule_interval=timedelta(days=1),

)

collect_and_save_data_task = PythonOperator(
    task_id='collect_sedes_bancos',
    python_callable=collect_bancos,
    dag=dag,
)

collect_and_save_data_task
