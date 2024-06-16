from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db

def collect_and_save_data(**kwargs):
    try:
        data = datetime.now().strftime('%d-%m-%Y')
        url = f"https://olinda.bcb.gov.br/olinda/servico/BcBase/versao/v2/odata/EntidadesSupervisionadas(dataBase=@dataBase)?@dataBase='{data}'&$top=10000&$format=json"

        # Fazer a requisição para obter os dados
        print(f"Fetching data from URL: {url}")
        requisicao = requests.get(url)
        print(f"Response status code: {requisicao.status_code}")

        if requisicao.status_code != 200:
            raise Exception(f"Failed to fetch data: {requisicao.text}")

        info = requisicao.json()
        print("Data fetched successfully.")

        # Conectar ao banco de dados PostgreSQL
        conexao = connect_db()
        curs = conexao.cursor()


        data_insert = []

        for dado in info['value']:
            data_insert.append((
                dado['dataBase'],
                dado['codigoIdentificadorBacen'], 
                dado['codigoSisbacen'], 
                dado['siglaISO3digitos'], 
                dado['nomeDoPais'], 
                dado['nomeDaUnidadeFederativa'], 
                dado['codigoDoMunicipioNoIBGE'], 
                dado['nomeDoMunicipio'], 
                dado['nomeEntidadeInteresse'], 
                dado['nomeEntidadeInteresseNaoFormatado'], 
                dado['codigoCNPJ14'], 
                dado['codigoCNPJ8'], 
                dado['codigoTipoSituacaoPessoaJuridica'], 
                dado['descricaoTipoSituacaoPessoaJuridica'], 
                dado['codigoTipoEntidadeSupervisionada'], 
                dado['descricaoTipoEntidadeSupervisionada'], 
                dado['codigoNaturezaJuridica'], 
                dado['descricaoNaturezaJuridica'],
                dado['codigoEsferaPublica'], 
                dado['nomeReduzido'], 
                dado['siglaDaPessoaJuridica'], 
                dado['nomeFantasia'], 
                dado['indicadorEsferaPublica']
            ))

        print("Número de novos registros a serem inseridos:", len(data_insert))

        if data_insert:
            sql = """
                INSERT INTO info_cadastral_entidades (
                    data_base, codigo_cadastro_bacen, codigo_sisbacen, codigo_pais_sede, 
                    nome_pais_sede, nome_uf_sede, codigo_municipio_sede, nome_municipio_sede, 
                    nome_entidade, nome_entidade_nao_formatado, cnpj, cnpj_raiz, codigo_situacao, 
                    descricao_situacao, codigo_tipo_entidade_segmento, nome_tipo_entidade, 
                    codigo_natureza_juridica, descricao_natureza_juridica, codigo_esfera_publica, 
                    nome_reduzido, sigla_entidade, nome_fantasia, empresa_publica
                ) VALUES %s
                ON CONFLICT (codigo_cadastro_bacen, codigo_sisbacen, codigo_pais_sede, 
                             nome_pais_sede, nome_uf_sede, codigo_municipio_sede, nome_municipio_sede, 
                             nome_entidade, nome_entidade_nao_formatado, cnpj, cnpj_raiz, codigo_situacao, 
                             descricao_situacao, codigo_tipo_entidade_segmento, nome_tipo_entidade, 
                             codigo_natureza_juridica, descricao_natureza_juridica, codigo_esfera_publica, 
                             nome_reduzido, sigla_entidade, nome_fantasia, empresa_publica) 
                DO NOTHING
            """

            execute_values(curs, sql, data_insert)
            conexao.commit()

        conexao.close()
    except Exception as e:
        print(f"Erro ao coletar e salvar dados: {e}")

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'info_cadastral_entidades',
    default_args=default_args,
    description='DAG para coleta e salvamento de dados',
    schedule_interval=timedelta(days=1),
)

# Tarefa para coletar e salvar os dados
collect_and_save_data_task = PythonOperator(
    task_id='collect_and_save_data_task',
    python_callable=collect_and_save_data,
    dag=dag,
)

collect_and_save_data_task
