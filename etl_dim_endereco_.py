import os
from pyspark.sql import SparkSession
from generate_df import table_to_df
from IPython.display import display, HTML
from tabulate import tabulate
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from unidecode import unidecode

# Definir o caminho do Spark corretamente
os.environ['SPARK_HOME'] = '/home/daiane/spark-3.5.1-bin-hadoop3/'

# Definir o caminho do Java corretamente
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-1.17.0-openjdk-amd64/'

# Iniciar uma sessão Spark
spark = SparkSession.builder \
    .appName("Exemplo Spark") \
    .getOrCreate()

# Testar a sessão Spark
spark

df_agencias = table_to_df('finance_raw_data', 'agencias', spark)
df_bancos = table_to_df('finance_raw_data', 'bancos', spark)
df_cooperativas_credito = table_to_df('finance_raw_data', 'cooperativas_credito', spark)
df_sociedades = table_to_df('finance_raw_data', 'sociedades', spark)
df_address_adm_consorcio = table_to_df('finance_raw_data', 'administradoras_consorcio', spark)

#colunas para a dim_endereco: id, cnpj, address_type, registration_date, date_end, street,
#complement, number,  neighborhood, city, postalcode, state

colunas = ["cnpj", "data", "endereco","complemento", "bairro",  "municipio","cep", "uf"]


df_address_adm_consorcio_selected = df_address_adm_consorcio.select(colunas).withColumn("address_type", F.lit("SEDE"))
df_bancos_selected = df_bancos.select(colunas).withColumn("address_type", F.lit("SEDE"))
df_cooperativas_credito_selected = df_cooperativas_credito.select(colunas).withColumn("address_type", F.lit("SEDE"))
df_sociedades_selected = df_sociedades.select(colunas).withColumn("address_type", F.lit("SEDE"))
df_agencias_selected = df_agencias.select(colunas).withColumn("address_type", F.lit("AGENCIA"))

dataframes_selected = [df_address_adm_consorcio_selected, df_bancos_selected, df_cooperativas_credito_selected, df_sociedades_selected, df_agencias_selected]

dataframes_selected = [
    df.withColumn("number", F.lit(None).cast("integer"))
      .withColumn("date_end", F.lit(None).cast("date"))
    for df in dataframes_selected
]

def unionAll(dataframes):
    return reduce(DataFrame.unionAll, dataframes)

df_enderecos = unionAll(dataframes_selected)

#colunas_ordem = ["cnpj",  "address_type", "data", "date_end", "endereco","complemento", "number", "bairro",  "municipio","cep", "uf"]

#df_enderecos = df_enderecos.select(colunas_ordem)

df_enderecos.persist()

# df_enderecos.count()

tipos_logradouros = ["AREA", "ACESSO", "ACAMPAMENTO", "AEROPORTO", "ALAMEDA", "AVENIDA", "BLOCO",
                     "CANAL", "CONDOMINIO", "DISTRITO", "ESTRADA", "RUA", "VIA", "TRAVESSA"]

#amostra_df = df_enderecos.sample(False, 0.5)

#df = amostra_df.filter(df_enderecos["endereco"].contains("PC. "))

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


# table = tabulate(df_tipo_corrigido.collect(), headers=df_enderecos_corrigido.columns, tablefmt='html')

 #table = tabulate(amostra_df.collect(), headers=amostra_df.columns, tablefmt='html')


# display(HTML(table))

# amostra_df = df_tipo_corrigido.sample(False, 0.1)

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


# df_numero_tratado_ = df_numero_tratado.filter(df_enderecos["endereco"].contains("EXTRAIRAPARTIRDAQUI-"))

# rows = df_numero_tratado.collect()

# columns = df_numero_tratado.columns

# table = tabulate(rows, headers=columns, tablefmt='html')

# display(HTML(table))


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
df_pos_numero_tratado = df_pos_numero_tratado.withColumn("complemento",
                                                       F.concat(F.col("apos_numero"), F.col("complemento")))

df_endereco_sem_numero = df_pos_numero_tratado.withColumn("endereco",
                                                      F.regexp_replace(F.col("endereco"), pattern, ""))    

caractere_especial = r'[^\w\s]$'

df_endereco_sem_especial = df_endereco_sem_numero.withColumn("endereco", 
                        F.regexp_replace(F.trim(F.col("endereco")), caractere_especial, ""))                                                                       
                                                                                   
df_pos_numero_tratado = df_endereco_sem_especial.drop("apos_numero")

df_pos_numero_tratado.persist()

df_pos_numero_tratado.createOrReplaceTempView("view_temporariaa")

df = spark.sql("SELECT endereco, number, complemento, data FROM view_temporariaa").limit(50)

# df = spark.sql(" SELECT apos_numero, COUNT(*) as freq from view_temporariaa group by apos_numero order by freq desc ").show(50)

# df = spark.sql("select * from view_temporariaa ")

table = tabulate(df.collect(), headers=df.columns, tablefmt='html')

# display(HTML(table))

# df_pos_numero_tratado.printSchema()

def clean_accent_(texto):
    return unidecode(texto) if texto else None

clean_accent = F.udf(clean_accent_, F.StringType())

df_sem_acento = df_pos_numero_tratado.withColumn("endereco", F.upper(clean_accent(F.col("endereco"))))\
                                     .withColumn("complemento", F.upper(clean_accent(F.col("complemento"))))\
                                     .withColumn("uf", F.upper(clean_accent(F.col("uf"))))\
                                    .withColumn("municipio", F.upper(clean_accent(F.col("municipio"))))

# df_sem_acento.show(20, truncate=False)

all_columns = df_sem_acento.columns

colunas_ = list(filter(lambda col: col != "data", all_columns))

df_unique = df_sem_acento.dropDuplicates(subset=colunas_)

# df_grouped = df_unique.groupBy(colunas_).count()

# df_duplicates = df_grouped.filter(F.col("count") > 1)


# if df_duplicates.count() > 0:
#     print("Há linhas duplicadas no DataFrame.")
#     print(df_duplicates.count() )
# else:
#     print("Não há linhas duplicadas no DataFrame.")

df_data_formatada = df_unique.withColumn("data", F.to_date(F.col("data")))

df_data_formatada_sample = df_data_formatada.sample(False, 0.1)

# table = tabulate(df_data_formatada_sample.collect(), headers=df_data_formatada_sample.columns, tablefmt='html')

# display(HTML(table))

df_final = df_data_formatada.select(df_data_formatada["data"].alias("registration_date"),
                                    df_data_formatada["endereco"].alias("street"),
                                     df_data_formatada["complemento"].alias("complement"),
                                    df_data_formatada["bairro"].alias("neighborhood"),
                                     df_data_formatada["municipio"].alias("city"),
                                    df_data_formatada["cep"].alias("postalcode"),
                                     df_data_formatada["uf"].alias("state"),
                                   df_data_formatada["cnpj"],
                                   df_data_formatada["address_type"],
                                   df_data_formatada["number"],
                                   df_data_formatada["date_end"])
# df_final.printSchema()
