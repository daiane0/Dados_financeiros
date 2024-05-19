#Coleta dos dados de 2017 até 08 de maio de 2024

from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values
from conexao_db import connect_db

conexao = connect_db

curs = conexao.cursor()

curs.execute("""
             create table if not exists info_cadastral_entidades (
                data_base varchar,
                codigo_cadastro_bacen varchar,
                codigo_sisbacen varchar,
                codigo_pais_sede varchar,
                nome_pais_sede varchar,
                nome_uf_sede varchar,
                codigo_municipio_sede varchar,
                nome_municipio_sede varchar,
                nome_entidade varchar,
                nome_entidade_nao_formatado varchar,
                cnpj varchar,
                cnpj_raiz varchar,
                codigo_situacao varchar,
                descricao_situacao varchar,
                codigo_tipo_entidade_segmento varchar,
                nome_tipo_entidade varchar,
                codigo_natureza_juridica varchar,
                descricao_natureza_juridica varchar,
                codigo_esfera_publica varchar,
                nome_reduzido varchar,
                sigla_entidade varchar,
                nome_fantasia varchar,
                empresa_publica varchar
                
                )"""
                 )
    
data_inicio = datetime(2017, 1, 1)
data_atual = datetime.now()
data = data_inicio
data_insert = []

data_formatada = data.strftime('%d-%m-%Y')

# # Construa a URL concatenando a data formatada
# url = f"https://olinda.bcb.gov.br/olinda/servico/BcBase/versao/v2/odata/EntidadesSupervisionadas(dataBase=@dataBase)?@dataBase={data_formatada}&$top=100&$format=json"

while data <= data_atual:
        # URL da API com a data dinâmica
    url = f"https://olinda.bcb.gov.br/olinda/servico/BcBase/versao/v2/odata/EntidadesSupervisionadas(dataBase=@dataBase)?@dataBase='{data_formatada}'&$top=100&$format=json"

    requisicao = requests.get(url)
    info = requisicao.json()

    existing_rows = set()

    curs.execute("select data_base, codigo_cadastro_bacen, codigo_sisbacen, codigo_pais_sede, nome_pais_sede, nome_uf_sede, codigo_municipio_sede, nome_municipio_sede, nome_entidade, nome_entidade_nao_formatado, cnpj, cnpj_raiz, codigo_situacao, descricao_situacao, codigo_tipo_entidade_segmento, nome_tipo_entidade, codigo_natureza_juridica, descricao_natureza_juridica, codigo_esfera_publica, nome_reduzido, sigla_entidade, nome_fantasia, empresa_publica from info_cadastral_entidades")

    for row in curs.fetchall():
        existing_rows.add(row)

    # Preparar dados para inserção
    

    for dado in info['value']:
    # Criar uma versão da tupla atual sem o primeiro item
        dado_without_first_item = tuple(list(dado.values())[1:])
    
    # Verificar se a tupla (sem o primeiro item) está presente no conjunto existing_rows
        if dado_without_first_item not in existing_rows:
                data_insert.append((
                dado['database'],
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


 
        sql = """
          INSERT INTO info_cadastral_entidades (
            data_base, 
            codigo_cadastro_bacen, 
            codigo_sisbacen, 
            codigo_pais_sede, 
            nome_pais_sede, 
            nome_uf_sede, 
            codigo_municipio_sede, 
            nome_municipio_sede, 
            nome_entidade, 
            nome_entidade_nao_formatado, 
            cnpj, 
            cnpj_raiz, 
            codigo_situacao, 
            descricao_situacao, 
            codigo_tipo_entidade_segmento, 
            nome_tipo_entidade, 
            codigo_natureza_juridica, 
            descricao_natureza_juridica, 
            codigo_esfera_publica, 
            nome_reduzido, 
            sigla_entidade, 
            nome_fantasia, 
            empresa_publica
          ) VALUES %s
          """
        # Executar a inserção dos dados
        execute_values(curs, sql, data_insert)

        conexao.commit()
        

        data += timedelta(days=1)

conexao.close()