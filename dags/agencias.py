from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db


def collect_and_save_data():

    url = "https://olinda.bcb.gov.br/olinda/servico/Informes_Agencias/versao/v1/odata/Agencias?$top=100000&$format=json"


    requisicao = requests.get(url)
    info = requisicao.json()

    conexao = connect_db()

    curs = conexao.cursor()
    

    data_insert = []

    for dado in info['value']:

        data_insert.append((dado['CnpjBase'],
                                dado['CnpjSequencial'],
                                dado['CnpjDv'],
                                dado['NomeIf'],
                                dado['Segmento'], 
                                dado['CodigoCompe'], 
                                dado['NomeAgencia'], 
                                dado['Endereco'],
                                dado['Numero'], 
                                dado['Complemento'], 
                                dado['Bairro'], 
                                dado['Cep'], 
                                dado['MunicipioIbge'],
                                dado['Municipio'], 
                                dado['UF'], 
                                dado['DataInicio'], 
                                dado['DDD'], 
                                dado['Telefone'], 
                                dado['Posicao'] ))
            
    print("Número de novos registros a serem inseridos:", len(data_insert))

    if data_insert:
        sql = "INSERT INTO agencias (cnpj , sequencial_cnpj , digitos_verificadores_cnpj , nome_da_instituicao , segmento , cod_comp , nome_agencia ,endereco , numero , complemento , bairro , cep , cod_ibge_municipio , municipio , uf , data_inicio , ddd , fone , posicao) VALUES %s ON CONFLICT (cnpj , sequencial_cnpj , digitos_verificadores_cnpj , nome_da_instituicao , segmento , cod_comp , nome_agencia ,endereco , numero , complemento , bairro , cep , cod_ibge_municipio , municipio , uf , data_inicio , ddd , fone ) DO NOTHING"

        print("Inserindo novos registros na tabela.")

        execute_values(curs, sql, data_insert)
        conexao.commit()

    conexao.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'agencias',
    default_args=default_args,
    description='DAG para coleta e salvamento de dados',
    schedule_interval=timedelta(days=1),
)

collect_and_save_data_task = PythonOperator(
    task_id='collect_agencias',
    python_callable=collect_and_save_data,
    dag=dag,
)

collect_and_save_data_task
