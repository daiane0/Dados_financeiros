from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import generate_df as G
from unidecode import unidecode
from pyspark.sql.window import Window
from config import db_config
from IPython.display import display, HTML
from tabulate import tabulate

spark = SparkSession.builder.appName("finance_data_processing")\
.config("spark.jars","/home/daiane/Downloads/postgresql-42.7.3.jar").getOrCreate()

def cnpj_calculate_(cnpj_base_input):
    #adiciona "0001" e calcula dígitos verificadores, retorna cnpj com 14 dígitos
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    cnpj1 = cnpj_base_input + "0001"
    cnpj_base = cnpj1.zfill(12)

    #1° dígito verficador
    soma1 = sum(int(digito) * peso for digito, peso in zip(cnpj_base, pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1

    #2° dígito verificador
    soma2 = sum(int(digito) * peso for digito, peso in zip(cnpj_base + str(digito1), pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2

    cnpj = cnpj_base  + str(digito1) + str(digito2)
    return cnpj

cnpj_calculate = F.udf(cnpj_calculate_, T.StringType())

df_info_cadastral = G.table_to_df("finance_raw_data", "info_cadastral_entidades", spark)
df_info_cadastral.persist()

df_info_cadastral_select = df_info_cadastral.select(
    "cnpj", "data", "cnpj_raiz", "nome_tipo_entidade",
    "descricao_natureza_juridica", "descricao_situacao", 
    "empresa_publica", "codigo_sisbacen", 
    "nome_entidade", "nome_fantasia"
)
#formatacao da data

df_info_cadastral_select = df_info_cadastral_select.withColumn("data_format", F.to_date(df_info_cadastral_select["data"], "yyyy-MM-dd"))
df_info_cadastral_select = df_info_cadastral_select.filter(df_info_cadastral_select["cnpj_raiz"].isNotNull()).drop("data")

#remocao de linhas duplicadas
df_info_ordered = df_info_cadastral_select.orderBy(F.col("data_format").desc())
columns_info = df_info_ordered.columns
columns_info.remove("data_format")

df_unique_info_ = df_info_ordered.dropDuplicates(subset=columns_info)

   
# config_dim_empresa = db_config("dw_finance", "dim_empresa")

# df_final.write.mode("append").format("jdbc").options(
#     url=config_dim_empresa["url"],
#     dbtable=config_dim_empresa["dbtable"],
#     user=config_dim_empresa["user"],
#     password=config_dim_empresa["password"],
#     driver=config_dim_empresa["driver"]
# ).save()

