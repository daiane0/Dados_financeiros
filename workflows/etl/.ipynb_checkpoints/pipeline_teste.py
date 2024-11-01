from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import generate_df as G
from unidecode import unidecode
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("finance_data_processing")\
    .config("spark.jars", "/home/daiane/Downloads/postgresql-42.7.3.jar").getOrCreate()

def cnpj_calculate_(cnpj_base_input):
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    cnpj1 = cnpj_base_input + "0001"
    cnpj_base = cnpj1.zfill(12)

    soma1 = sum(int(digito) * peso for digito, peso in zip(cnpj_base, pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1

    soma2 = sum(int(digito) * peso for digito, peso in zip(cnpj_base + str(digito1), pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2

    cnpj = cnpj_base + str(digito1) + str(digito2)
    return cnpj

cnpj_calculate = F.udf(cnpj_calculate_, T.StringType())

df_info_cadastral = G.table_to_df("finance_raw_data", "info_cadastral_entidades", spark)
df_info_cadastral.persist()

df_info_cadastral_select = df_info_cadastral.select(
    "cnpj", "data", "cnpj_raiz", "nome_tipo_entidade",
    "descricao_natureza_juridica", "descricao_situacao", 
    "empresa_publica", "codigo_sisbacen", "nome_tipo_entidade", 
    "descricao_situacao", "nome_entidade", "nome_fantasia"
)

# Formatação da data
df_info_cadastral_select = df_info_cadastral_select.withColumn(
    "data_format", F.to_date(df_info_cadastral_select["data"], "yyyy-MM-dd")
).filter(F.col("cnpj_raiz").isNotNull()).drop("data")

# Remoção de linhas duplicadas
df_info_ordered = df_info_cadastral_select.orderBy(F.col("data_format").desc())
columns_info = df_info_ordered.columns
columns_info.remove("data_format")
df_unique_info_ = df_info_ordered.dropDuplicates(subset=columns_info)

# Limpeza de acentos
def clean_accent_(texto):
    return unidecode(texto) if texto else None

clean_accent = F.udf(clean_accent_, T.StringType())

# Modificação: Aplicação da UDF clean_accent e atribuição ao DataFrame
for column, dtype in df_unique_info_.dtypes:
    if dtype == "string":
        df_unique_info_ = df_unique_info_.withColumn(column, F.upper(F.trim(clean_accent(F.col(column)))))

# Cálculo do CNPJ completo a partir dos 8 primeiros dígitos
df_cnpj_matriz = df_unique_info_.withColumn("cnpj_raiz", cnpj_calculate(F.col("cnpj_raiz")))

# Definição do tipo de controle
tipo_controle = df_cnpj_matriz.withColumn(
    "tipo_controle", 
    F.when(F.col("empresa_publica") == "1", "EMPRESA PUBLICA").otherwise("EMPRESA PRIVADA")
)

# Seleção e renomeação de colunas
df_final = tipo_controle.select(
    "cnpj_raiz", "nome_tipo_entidade", "tipo_controle", "descricao_situacao", 
    "data_format", "nome_entidade", "nome_fantasia", "descricao_natureza_juridica"
).withColumn("date_end", F.lit(None).cast("date")).withColumnRenamed(
    "cnpj_raiz", "cnpj"
).withColumnRenamed(
    "nome_tipo_entidade", "entity_type"
).withColumnRenamed(
    "tipo_controle", "control_type"
).withColumnRenamed(
    "descricao_situacao", "legal_status"
).withColumnRenamed(
    "data_format", "registration_date"
).withColumnRenamed(
    "nome_entidade", "registration_name"
).withColumnRenamed(
    "nome_fantasia", "company_name"
).withColumnRenamed(
    "descricao_natureza_juridica", "company_type"
)

# Cálculo de date_end com window function
window_spec = Window.partitionBy("cnpj").orderBy("registration_date")

df_final_with_next = df_final.withColumn(
    "next_registration_date", F.lead("registration_date").over(window_spec)
)

df_final_with_date_end = df_final_with_next.withColumn(
    "date_end", F.expr("date_add(next_registration_date, -1)")
).drop("next_registration_date")

df_final_with_date_end.show()
