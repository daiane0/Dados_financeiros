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
        "conglomerado",
        "codigo_conglomerado_financeiro",
        "codigo_conglomerado_prudencial",
        "tcb",
        "tc",
        "ti",
        "cidade",
        "uf",
        "data",
        "rendas_operacoes_credito_a1",
        "rendas_operacoes_arrendamento_mercantil_a2",
        "rendas_operacoes_com_tvm_a3",
        "rendas_operacoes_instrumentos_financeiros_derivativos_a4",
        "resultado_operacoes_cambio_a5",
        "rendas_aplicacoes_compulsorias_a6",
        "receitas_intermediacao_financeira_soma_a1_a2_a3_a4_a5_a6",
        "despesas_captacao_b1",
        "despesas_obrigacoes_por_emprestimos_e_repasses_b2",
        "despesas_operacoes_arrendamento_mercantil_b3",
        "resultado_operacoes_cambio_b4",
        "resultado_de_provisao_para_creditos_dificil_liquidacao_b5",
        "despesas__intermediacao_financeira_soma_b1_b2_b3_b4_b5",
        "resultao_intermediacao_financeira_c_soma_a_b",
        "rendas_prestacao_servico_d1",
        "rendas_tarifas_bancarias_d2",
        "despesas_de_pessoal_d3",
        "despesas_administrativas_d4",
        "despesas_tributarias_d5",
        "resultado_de_participacoes_d6",
        "outras_receitas_operacionais_d7",
        "outras_receitas_operacionais_d8",
        "soma_d1_d2_d3_d4_d5_d6_d7_d8",
        "resultado_operacional_soma_c_d",
        "resultado_nao_operacional_f",
        "g",
        "imposto_renda_contribuicao_social_h",
        "participacao_lucros_i",
        "lucro_liquido_soma_g_h_i",
        "juros_capital_social_cooperativas_k"
    ]      

    df = pd.read_csv(StringIO('\n'.join(linhas)), header=None, skiprows=3, delimiter=';', names=colunas, usecols=range(41))



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

