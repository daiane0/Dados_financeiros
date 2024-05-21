from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db

def collect_and_save_data(**kwargs):

    data = datetime.now().strftime('%d-%m-%Y')

    url = f"https://olinda.bcb.gov.br/olinda/servico/BcBase/versao/v2/odata/EntidadesSupervisionadas(dataBase=@dataBase)?@dataBase='{data}'&$top=10000&$format=json"

    requisicao = requests.get(url)
    info = requisicao.json()

    conexao = connect_db()

    curs = conexao.cursor()

    curs.execute("""
        CREATE TABLE IF NOT EXISTS info_cadastral_entidades (
            data_base VARCHAR,
            codigo_cadastro_bacen VARCHAR,
            codigo_sisbacen VARCHAR,
            codigo_pais_sede VARCHAR,
            nome_pais_sede VARCHAR,
            nome_uf_sede VARCHAR,
            codigo_municipio_sede VARCHAR,
            nome_municipio_sede VARCHAR,
            nome_entidade VARCHAR,
            nome_entidade_nao_formatado VARCHAR,
            cnpj VARCHAR,
            cnpj_raiz VARCHAR,
            codigo_situacao VARCHAR,
            descricao_situacao VARCHAR,
            codigo_tipo_entidade_segmento VARCHAR,
            nome_tipo_entidade VARCHAR,
            codigo_natureza_juridica VARCHAR,
            descricao_natureza_juridica VARCHAR,
            codigo_esfera_publica VARCHAR,
            nome_reduzido VARCHAR,
            sigla_entidade VARCHAR,
            nome_fantasia VARCHAR,
            empresa_publica VARCHAR
        )
    """)

    curs.execute("""
        SELECT codigo_cadastro_bacen, codigo_sisbacen, codigo_pais_sede, 
               nome_pais_sede, nome_uf_sede, codigo_municipio_sede, nome_municipio_sede, 
               nome_entidade, nome_entidade_nao_formatado, cnpj, cnpj_raiz, codigo_situacao, 
               descricao_situacao, codigo_tipo_entidade_segmento, nome_tipo_entidade, 
               codigo_natureza_juridica, descricao_natureza_juridica, codigo_esfera_publica, 
               nome_reduzido, sigla_entidade, nome_fantasia, empresa_publica 
        FROM info_cadastral_entidades
    """)

    existing_rows = set(curs.fetchall())

    data_insert = []

    for dado in info['value']:
        dado_without_first_item = (
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
            dado['siglaDaPessoaJuridica'], 
            dado['codigoEsferaPublica'], 
            dado['nomeReduzido'], 
            dado['nomeFantasia'], 
            dado['indicadorEsferaPublica']
        )
        
        if dado_without_first_item not in existing_rows:
            data_insert.append((
                dado['DataBase'],
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
                dado['siglaDaPessoaJuridica'], 
                dado['codigoEsferaPublica'], 
                dado['nomeReduzido'], 
                dado['nomeFantasia'], 
                dado['indicadorEsferaPublica']
            ))

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
        """
        execute_values(curs, sql, data_insert)

    conexao.close()

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

collect_and_save_data_task = PythonOperator(
    task_id='collect_and_save_data_task',
    python_callable=collect_and_save_data,
    dag=dag,
)

collect_and_save_data_task
