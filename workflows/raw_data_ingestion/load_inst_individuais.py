import pandas as pd
from io import StringIO 
from sqlalchemy import create_engine
from generate_df import url_to_connect
import os
from urllib.parse import quote_plus

csv_file_path = "/home/daiane/projetos/finance_data/data/demonstracao_resultados_individuais/dados_09-2004.csv"

folder_path = "/home/daiane/projetos/finance_data/data/demonstracao_resultados_individuais"


def process_csv_file(csv_file_path):
    linhas = []

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        for line in file:
            if line.startswith('TCB - Tipo de Consolidado Bancário'):
                break
            linhas.append(line.strip())

    colunas = [
    "instituicao",
    "codigo",
    "tcb",
    "sr",
    "TD",
    "tc",
    "cidade",
    "uf",
    "data",
    "Rendas_operacoes_credito_a1",
    "Rendas_Operacoes_Arrendamento_Mercantil_a2",
    "Rendas_Operacoes_com_TVM_a3",
    "Rendas_Operacoes_Instrumentos_Financeiros_Derivativos_a4",
    "Resultado_Operacoes_Cambio_a5",
    "Rendas_Aplicacoes_Compulsorias_a6",
    "Receitas_Intermediacao_Financeira_soma_a1_a2_a3_a4_a5_a6",
    "despesas_captacao_b1",
    "Despesas_Obrigacoes_por_Emprestimos_e_Repasses_b2",
    "Despesas_Operacoes_Arrendamento_Mercantil_b3",
    "Resultado_Operacoes_Cambio_b4",
    "Resultado_de_Provisao_para_Creditos_Dificil_Liquidacao_b5",
    "Despesas__Intermediacao_Financeira_soma_b1_b2_b3_b4_b5",
    "resultao_intermediacao_financeira_c_soma_a_b",
    "rendas_prestacao_servico_d1",
    "rendas_tarifas_bancárias_d2",
    "despesas_de_pessoal_d3",
    "despesas_administrativas_d4",
    "despesas_tributarias_d5",
    "resultado_de_participacoes_d6",
    "outras_receitas_operacionais_d7",
    "outras_receitas_operacionais_d8",
    "soma_d1_d2_d3_d4_d5_d6_d7_d8",
    "resultado_operacional_soma_c_d",
    "resultado_nao_operacional_f",
    "resultado_antes_tributacao",
    "lucro_e_participacao_soma_e_f",
    "imposto_renda_contribuicao_social_h",
    "participacao_lucros_i",
    "lucro_liquido_soma_g_h_i",
    "juros_capital_social_cooperativas_k"
]       

    df = pd.read_csv(StringIO('\n'.join(linhas)), header=None, skiprows=3, delimiter=';', names=colunas, usecols=range(40))



    df.to_sql(
        name='demonstracao_resultado_receita_lucro',
        con=create_engine(url_to_connect('finance_raw_data')),
        index=False, 
        if_exists='append')



for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        csv_file_path = os.path.join(folder_path, file_name)
        process_csv_file(csv_file_path)
        print(f"Arquivo {file_name} inserido.")

# print(df.shape[1])
# # print(df.head())
# print(df.columns)

