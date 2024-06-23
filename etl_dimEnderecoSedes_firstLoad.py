# PRIMEIRA CARGA NO DW
from pyspark.sql import SparkSession
import generate_df as G
from IPython.display import display, HTML
from tabulate import tabulate
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from unidecode import unidecode
import table_load_control as Load_Control
from config import db_config

spark = SparkSession.builder.appName("finance_data_processing")\
.config("spark.jars","/home/daiane/Downloads/postgresql-42.7.3.jar").getOrCreate()

# print(spark)

#colunas para a dim_endereco: id, cnpj, address_type, registration_date, date_end, street,
#complement, number,  neighborhood, city, postalcode, state

df_agencias = G.table_to_df("finance_raw_data", "agencias", spark)
df_bancos = G.table_to_df("finance_raw_data", "bancos", spark)
df_cooperativas_credito = G.table_to_df("finance_raw_data", "cooperativas_credito", spark)
df_sociedades = G.table_to_df("finance_raw_data", "sociedades", spark)
df_address_adm_consorcio = G.table_to_df("finance_raw_data", "administradoras_consorcio", spark)

colunas = ["cnpj", "data", "endereco","complemento", "bairro",  "municipio","cep", "uf"]

df_address_adm_consorcio_selected = df_address_adm_consorcio.select(colunas)
df_bancos_selected = df_bancos.select(colunas)
df_cooperativas_credito_selected = df_cooperativas_credito.select(colunas)
df_sociedades_selected = df_sociedades.select(colunas)

dataframes_selected = [df_address_adm_consorcio_selected, df_bancos_selected, df_cooperativas_credito_selected, df_sociedades_selected]

dataframes_selected = [
    df.withColumn("number", F.lit(None))
      .withColumn("date_end", F.lit(None))
    for df in dataframes_selected]

def unionAll(dataframes):
    return reduce(DataFrame.unionAll, dataframes)

df_enderecos = unionAll(dataframes_selected).withColumn("address_type", F.lit("SEDE"))

df_enderecos.persist()


tipos_logradouros = ["AREA", "ACESSO", "ACAMPAMENTO", "AEROPORTO", "ALAMEDA", "AVENIDA", "BLOCO",
                     "CANAL", "CONDOMINIO", "DISTRITO", "ESTRADA", "RUA", "VIA", "TRAVESSA"]

#amostra_df = df_enderecos.sample(False, 0.5)

df_tipo_corrigido = df_enderecos.withColumn(
    "endereco",
    F.when(
        F.col("endereco").rlike(r"^R\. ") | F.col("endereco").rlike(r"^R "),
        F.regexp_replace(F.col("endereco"), r"^R\.? ", "RUA ")
    ).when(
        F.col("endereco").rlike(r"^AV\. ") | F.col("endereco").rlike(r"^AV "),
        F.regexp_replace(F.col("endereco"), r"^AV\.? ", "AVENIDA ")
    ).when(
        F.col("endereco").rlike(r"^TV\. ") | F.col("endereco").rlike(r"^TV "),
        F.regexp_replace(F.col("endereco"), r"^TV\.? ", "TRAVESSA ")
    ).when(
        F.col("endereco").rlike(r"^PC "),
        F.regexp_replace(F.col("endereco"), r"^PC ", "PRACA ")
    ).otherwise(F.col("endereco"))
)

df_tipo_corrigido.persist()


regex = r"(\d+(?:\.\d+)?)"

tipos_logradouros = ["AREA", "ACESSO", "ACAMPAMENTO", "AEROPORTO", "ALAMEDA", "AVENIDA", "BLOCO",
                     "CANAL", "CONDOMINIO", "DISTRITO", "ESTRADA", "RUA", "VIA", "TRAVESSA"]

df_numero = df_tipo_corrigido.withColumn("number",
    F.when(
        (F.col("endereco").like("%BR %")) |
        (F.col("endereco").like("%BR/%")) |
        (F.col("endereco").like("%RODOVIA%")) |
        (F.col("endereco").rlike(r"^\b(" + "|".join(tipos_logradouros) + r")\b \d+")) |
        (F.col("endereco").rlike(r"^\b(" + "|".join(tipos_logradouros) + r")\b \d+[A-Z]*$")) |
        ((F.col("endereco").like("%QUADRA%")) & (~F.col("endereco").like("%LOTE%"))) |
        (F.col("endereco").rlike(r'\d+-\d+')),
        ""
    ).otherwise(
        F.when(
            (F.col("endereco").like("%QUADRA%")) & (F.col("endereco").like("%LOTE%")),
            F.regexp_extract(F.col("endereco"), r".LOTE (\d+)", 1)
        ).when(
            (F.col("endereco").like("%N %")) | (F.col("endereco").like("%N.")) | (F.col("endereco").like("%Nº")),
            F.regexp_extract(F.col("endereco"), r"N[ .º]?(\d+)", 1)
        ).otherwise(
            F.regexp_extract(F.col("endereco"), regex, 0)
        )
    )
).withColumn("endereco",
    F.when(
        (F.col("number") != "") &
        ~(F.col("endereco").rlike(r'\d+-\d+')),  
        F.regexp_replace(F.col("endereco"), F.col("number"), "EXTRAIRAPARTIRDAQUI")
    ).otherwise(F.col("endereco")))

df_numero.persist()


non_empty_count = df_numero.filter((F.col("number").isNotNull()) & (F.col("number") != "")).count()

empty_count = df_numero.filter((F.col("number").isNull()) | (F.col("number") == "")).count()

total_count = df_numero.count()

non_empty_percentage = (non_empty_count / total_count) * 100
empty_percentage = (empty_count / total_count) * 100

print(f"Registros não vazios: {non_empty_count} ({non_empty_percentage:.2f}%)")
print(f"Registros vazios: {empty_count} ({empty_percentage:.2f}%)")


pattern = "EXTRAIRAPARTIRDAQUI(.*)"

df_pos_numero = df_numero.withColumn("apos_numero", F.regexp_extract("endereco", pattern, 1))

df_pos_numero_tratado = df_pos_numero.withColumn("apos_numero", F.regexp_replace(F.col("apos_numero"), r'^[^a-zA-Z0-9]+', ''))

# Adicionar " - " ao final se a string não ficar vazia após a limpeza
df_pos_numero_tratado = df_pos_numero_tratado.withColumn(
    "apos_numero",
    F.when(F.col("apos_numero") != '', F.concat(F.col("apos_numero"), F.lit(' - '))).otherwise('')
)

#concatenando a coluna temporario tratada com o complemento
df_pos_numero_tratado = df_pos_numero_tratado.withColumn("complemento", F.concat(F.col("apos_numero"), F.col("complemento")))

df_endereco_sem_numero = df_pos_numero_tratado.withColumn("endereco", F.regexp_replace(F.col("endereco"), pattern, ""))    

caractere_especial = r'[^\w\s]$'

df_endereco_sem_especial = df_endereco_sem_numero.withColumn("endereco", 
                        F.regexp_replace(F.trim(F.col("endereco")), caractere_especial, ""))                                                                       
                                                                                   
df_pos_numero_tratado = df_endereco_sem_especial.drop("apos_numero")

df_pos_numero_tratado.persist()


def clean_accent_(texto):
    return unidecode(texto) if texto else None

clean_accent = F.udf(clean_accent_, F.StringType())

df_sem_acento = df_pos_numero_tratado.withColumn("endereco", F.upper(clean_accent(F.col("endereco"))))\
                                     .withColumn("complemento", F.upper(clean_accent(F.col("complemento"))))\
                                     .withColumn("uf", F.upper(clean_accent(F.col("uf"))))\
                                    .withColumn("municipio", F.upper(clean_accent(F.col("municipio"))))

all_columns = df_sem_acento.columns

colunas_ = list(filter(lambda col: col != "data", all_columns))

df_unique = df_sem_acento.dropDuplicates(subset=colunas_)

df_data_formatada = df_unique.withColumn("data", F.to_date(F.col("data")))

df_data_formatada_sample = df_data_formatada.sample(False, 0.1)

df_colunas_ = df_data_formatada.select(
    df_data_formatada["cnpj"].cast(T.StringType()),
    df_data_formatada["address_type"].cast(T.StringType()),
    df_data_formatada["data"].alias("registration_date").cast(T.DateType()),  
    df_data_formatada["date_end"].cast(T.DateType()),  
    df_data_formatada["endereco"].alias("street").cast(T.StringType()),
    df_data_formatada["complemento"].alias("complement").cast(T.StringType()),
    df_data_formatada["number"].cast(T.IntegerType()),  
    df_data_formatada["bairro"].alias("neighborhood").cast(T.StringType()),
    df_data_formatada["municipio"].alias("city").cast(T.StringType()),
    df_data_formatada["uf"].alias("state").cast(T.StringType()),
    df_data_formatada["cep"].alias("postalcode").cast(T.StringType())
)

df_colunas = df_colunas_.withColumn("branch_code", F.lit(None).cast(T.StringType()))

# df_colunas.printSchema()

df_final = df_colunas.withColumn("postalcode", F.regexp_replace(F.col("postalcode"), "-", ""))



max_date_row = df_final.agg(F.max(F.col("registration_date")).alias("max_date")).collect()[0]
last_date = max_date_row["max_date"]

print(last_date)

schema = T.StructType([
    T.StructField("load_date", T.DateType(), True),
    T.StructField("table_name", T.StringType(), True),
    T.StructField("data_type", T.StringType(), True)])

data = [(last_date, 'dim_endereco', 'sede')]

last_date_df = spark.createDataFrame(data, schema)

config_dim_endereco = db_config("dw_finance", "dim_endereco")

config_table_load_control = db_config("dw_finance", "dw_load_control")

df_final.write.mode("append").format("jdbc").options(
    url=config_dim_endereco["url"],
    dbtable=config_dim_endereco["dbtable"],
    user=config_dim_endereco["user"],
    password=config_dim_endereco["password"],
    driver=config_dim_endereco["driver"]
).save()


last_date_df.write.mode("append").format("jdbc").options(
    url=config_table_load_control["url"],
    dbtable=config_table_load_control["dbtable"],
    user=config_table_load_control["user"],
    password=config_table_load_control["password"],
    driver=config_table_load_control["driver"]
).save()

