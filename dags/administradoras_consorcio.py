
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db

def collect_and_save_data():
    url = "https://olinda.bcb.gov.br/olinda/servico/Instituicoes_em_funcionamento/versao/v1/odata/SedesConsorcios?$top=100000&$format=json"


    requisicao = requests.get(url)
    info = requisicao.json()

    conexao = connect_db()

    curs = conexao.cursor()


    curs.execute(
        """
        CREATE TABLE IF NOT EXISTS administradoras_consorcio (
            id SERIAL PRIMARY KEY,
            cnpj VARCHAR,
            nome_instituicao VARCHAR,
            endereco VARCHAR,
            complemento VARCHAR,
            bairro VARCHAR,
            cep VARCHAR,
            municipio VARCHAR,
            uf VARCHAR,
            ddd VARCHAR,
            telefone VARCHAR,
            email VARCHAR,
            sitio_internet VARCHAR,
            municipio_ibge VARCHAR,
            data_recebido timestamp default current_timestamp
        )
        """
    )

    # Verificar se os registros já existem na tabela
    existing_rows = set()
    curs.execute("SELECT cnpj, nome_instituicao, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, email, sitio_internet, municipio_ibge FROM administradoras_consorcio")
    for row in curs.fetchall():
        existing_rows.add(row)


    # Preparar dados para inserção
    data_insert = []
    for dado in info['value']:
        if tuple([dado['CNPJ'], dado['NOME_INSTITUICAO'], dado['ENDERECO'], dado['COMPLEMENTO'], dado['BAIRRO'], dado['CEP'], dado['MUNICIPIO'], dado['UF'], dado['DDD'], dado['TELEFONE'], dado['E_MAIL'], dado['SITIO_NA_INTERNET'], dado['MUNICIPIO_IBGE']]) not in existing_rows:
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
                dado['E_MAIL'], 
                dado['SITIO_NA_INTERNET'], 
                dado['MUNICIPIO_IBGE']
            ))

    # SQL para inserir os dados
    sql = "INSERT INTO administradoras_consorcio (cnpj, nome_instituicao, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, email, sitio_internet, municipio_ibge) VALUES %s"

    # Executar a inserção dos dados
    if data_insert:
        execute_values(curs, sql, data_insert)

    # Commit e fechar conexão
    conexao.commit()
    conexao.close()

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sedes_administradoras_consorcio',
    default_args=default_args,
    description='DAG para coleta e salvamento de dados',
    schedule_interval=timedelta(days=1),  # Executar a cada 24 horas
)

# Tarefa para coletar e salvar os dados
collect_and_save_data_task = PythonOperator(
    task_id='collect_and_save_data_task',
    python_callable=collect_and_save_data,
    dag=dag,
)

collect_and_save_data_task
