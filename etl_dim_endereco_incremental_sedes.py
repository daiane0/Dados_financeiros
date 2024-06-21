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
import table_load_control as T


os.environ['SPARK_HOME'] = '/home/daiane/spark-3.5.1-bin-hadoop3/'

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-1.17.0-openjdk-amd64/'

spark = SparkSession.builder \
    .appName("Exemplo Spark") \
    .getOrCreate()

spark

# data da última carga
last_load_date = T.get_last_load_date('dim_endereco', 'sede', spark)

df_bancos = table_to_df('finance_raw_data', 'bancos', spark).filter(col('data') > last_load_date)
df_cooperativas_credito = table_to_df('finance_raw_data', 'cooperativas_credito', spark).filter(col('data') > last_load_date)
df_sociedades = table_to_df('finance_raw_data', 'sociedades', spark).filter(col('data') > last_load_date)
df_address_adm_consorcio = table_to_df('finance_raw_data', 'administradoras_consorcio', spark).filter(col('data') > last_load_date)

#colunas para a dim_endereco: id, cnpj, address_type, registration_date, date_end, street,
#complement, number,  neighborhood, city, postalcode, state

colunas = ["cnpj", "data", "endereco","complemento", "bairro",  "municipio","cep", "uf", "cod_comp"]


df_address_adm_consorcio_selected = df_address_adm_consorcio.select(colunas)
df_bancos_selected = df_bancos.select(colunas))
df_cooperativas_credito_selected = df_cooperativas_credito.select(colunas)
df_sociedades_selected = df_sociedades.select(colunas)

dataframes_selected = [df_address_adm_consorcio_selected, df_bancos_selected, df_cooperativas_credito_selected, df_sociedades_selected]

dataframes_selected = [
    df.withColumn("number", F.lit(None).cast("integer"))
      .withColumn("date_end", F.lit(None).cast("date"))
    for df in dataframes_selected
]

def unionAll(dataframes):
    return reduce(DataFrame.unionAll, dataframes)

df_enderecos = unionAll(dataframes_selected).withColumn("address_type", F.lit("SEDE"))


df_enderecos.persist()


tipos_logradouros = ["AREA", "ACESSO", "ACAMPAMENTO", "AEROPORTO", "ALAMEDA", "AVENIDA", "BLOCO",
                     "CANAL", "CONDOMINIO", "DISTRITO", "ESTRADA", "RUA", "VIA", "TRAVESSA"]


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
        F.regexp_replace(F.col("endereco"), r"^PC ", "PRACA ")).otherwise(F.col("endereco")))

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
            F.regexp_extract(F.col("endereco"), regex, 0)))
).withColumn("endereco",
    F.when(
        (F.col("number") != "") &
        ~(F.col("endereco").rlike(r'\d+-\d+')),  
        F.regexp_replace(F.col("endereco"), F.col("number"), "EXTRAIRAPARTIRDAQUI")
    ).otherwise(F.col("endereco")))



df_numero.persist()

pattern = "EXTRAIRAPARTIRDAQUI(.*)"

df_pos_numero = df_numero.withColumn("apos_numero", F.regexp_extract("endereco", pattern, 1))

df_pos_numero_tratado = df_pos_numero.withColumn("apos_numero", F.regexp_replace(F.col("apos_numero"), r'^[^a-zA-Z0-9]+', ''))

df_pos_numero_tratado = df_pos_numero_tratado.withColumn("apos_numero",
    F.when(F.col("apos_numero") != '', F.concat(F.col("apos_numero"), F.lit(' - '))).otherwise(''))

df_pos_numero_tratado = df_pos_numero_tratado.withColumn("complemento",
                                                       F.concat(F.col("apos_numero"), F.col("complemento")))

df_endereco_sem_numero = df_pos_numero_tratado.withColumn("endereco",
                                                      F.regexp_replace(F.col("endereco"), pattern, ""))    

caractere_especial = r'[^\w\s]$'

df_endereco_sem_especial = df_endereco_sem_numero.withColumn("endereco", 
                        F.regexp_replace(F.trim(F.col("endereco")), caractere_especial, ""))                                                                       
                                                                                   
df_pos_numero_tratado = df_endereco_sem_especial.drop("apos_numero")

df_pos_numero_tratado.persist()

# df_pos_numero_tratado.createOrReplaceTempView("view_temporariaa")

# df = spark.sql("SELECT endereco, number, complemento, data FROM view_temporariaa").limit(50)

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


df_colunas = df_data_formatada.select(df_data_formatada["cnpj"],
                                      df_data_formatada["address_type"],
                                      df_data_formatada["data"].alias("registration_date"),
                                      df_data_formatada["date_end"],
                                      df_data_formatada["endereco"].alias("street"),
                                      df_data_formatada["complemento"].alias("complement"),
                                      df_data_formatada["number"],
                                      df_data_formatada["bairro"].alias("neighborhood"),
                                      df_data_formatada["municipio"].alias("city"),
                                      df_data_formatada["uf"].alias("state"),
                                      df_data_formatada["cep"].alias("postalcode"))


df_colunas.withColumn("branch_code", F.lit(None))

# df_final.printSchema()

df_final = df_colunas.withColumn("postalcode", F.regexp_replace(F.col("postalcode"), "-", ""))

teste = df_final.filter(F.col("postalcode").rlike("[^a-zA-Z0-9]"))

# --------------------------------------------------------------------

# Carregar dados do DW
dw_dim_endereco = table_to_df('dw_finance', 'dim_endereco', spark)

dw_dim_endereco.createOrReplaceTempView("dim_endereco")

df_final.createOrReplaceTempView("df_final")


 # Atualização dos registros existentes no DW usando SCD tipo 2

sql_update = """UPDATE dw
SET date_end = df_final
FROM dim_endereco dw
JOIN df_final ON dw.cnpj = df_final.cnpj
WHERE dw.date_end IS NULL; -- Considerando apenas os registros ativos sem data de fim"""

sql_insert = """

INSERT INTO dim_endereco (cnpj, address_type, registration_date, date_end, street, complement, number, neighborhood, city, state, postalcode, branch_code)
SELECT cnpj, address_type, registration_date, date_end, street, complement, number, neighborhood, city, state, postalcode, branch_code
FROM df_final;
"""

# Após todas as transformações, obtenha a data máxima dos dados processados
max_date = df_final.agg({"data": "max"})

spark.sql(T.update_last_load_date(max_date, "dim_endereco", "sede"))
spark.sql(sql_update)
spark.sql(sql_insert)
                                 
