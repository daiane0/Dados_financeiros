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

#limpeza de acentos 
def clean_accent_(texto):
    return unidecode(texto) if texto else None

clean_accent = F.udf(clean_accent_, F.StringType())

for column, dtype in df_unique_info_.dtypes:
    if dtype == "string":
        df_unique_info_ = df_unique_info_.withColumn(column, F.upper(F.trim(clean_accent(F.col(column)))))


#cnpj completoa partir dos 8 primeiros dígitos
df_cnpj_matriz = df_unique_info_.withColumn("cnpj_raiz", cnpj_calculate(df_unique_info_["cnpj_raiz"]))


#Definicao do tipo de controle
tipo_controle = df_cnpj_matriz.withColumn("tipo_controle", F.when(F.col("empresa_publica") == "1", "EMPRESA PUBLICA").otherwise("EMPRESA PRIVADA"))


df_final = (tipo_controle
            .select("cnpj_raiz", "nome_tipo_entidade", "tipo_controle","descricao_situacao", "data_format", "nome_entidade", "nome_fantasia", "descricao_natureza_juridica")
            .withColumn("data_final", F.lit(None).cast("date")))

df_final = (df_final.withColumnRenamed("cnpj_raiz", "cnpj")
            .withColumnRenamed("nome_tipo_entidade", "entity_type")
            .withColumnRenamed("tipo_controle", "control_type")
            .withColumnRenamed("descricao_situacao", "legal_status")
            .withColumnRenamed("data_format", "registration_date")
            .withColumnRenamed("data_final", "date_end")
            .withColumnRenamed("nome_entidade", "registration_name")
            .withColumnRenamed("nome_fantasia", "company_name")
            .withColumnRenamed("descricao_natureza_juridica", "company_type"))


#calculo de date_end com window function

window_spec = Window.partitionBy("cnpj").orderBy("registration_date")

# Cria uma coluna para a próxima data de registro para cada cnpj
df_final_with_next = df_final.withColumn("next_registration_date", F.lead("registration_date").over(window_spec))

# Preenche a coluna date_end com a próxima data de registro menos um dia ou null se nao tiver proxima data
df_final_with_date_end = df_final_with_next.withColumn("date_end", F.expr("date_add(next_registration_date, -1)"))

df_final = df_final_with_date_end.drop("next_registration_date")

df_sample = df_final.sample(False, 0.1).limit(50)
df_sample.show()

# tb = tabulate(df_final.collect(), headers=df_final.columns, tablefmt='html')

# display(HTML(tb))

# config_dim_empresa = db_config("dw_finance", "dim_empresa")

# df_final.write.mode("append").format("jdbc").options(
#     url=config_dim_empresa["url"],
#     dbtable=config_dim_empresa["dbtable"],
#     user=config_dim_empresa["user"],
#     password=config_dim_empresa["password"],
#     driver=config_dim_empresa["driver"]
# ).save()

