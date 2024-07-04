from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def etl_dim_endereco_sede():
    try:
        logger.info("Iniciando o Spark Session")
        spark = SparkSession.builder.appName("finance_data_processing")\
        .config("spark.jars","/home/daiane/Downloads/postgresql-42.7.3.jar").getOrCreate()

        logger.info("Lendo a tabela de controle de carga")
        last_load_date = G.table_to_df('dw_finance', 'dw_load_control')

        logger.info("Obtendo a última data de carga")
        date_row = last_load_date.agg(F.max(F.col("load_date")).alias("max_date")).collect()[0]
        last_load_date = date_row["max_date"]

        logger.info(f"Última data de carga obtida: {last_load_date}")

        logger.info("Lendo as tabelas de dados brutos")
        df_bancos = G.table_to_df('finance_raw_data', 'bancos', spark).filter(F.col('data') > last_load_date)
        df_cooperativas_credito = G.table_to_df('finance_raw_data', 'cooperativas_credito', spark).filter(F.col('data') > last_load_date)
        df_sociedades = G.table_to_df('finance_raw_data', 'sociedades', spark).filter(F.col('data') > last_load_date)
        df_address_adm_consorcio = G.table_to_df('finance_raw_data', 'administradoras_consorcio', spark).filter(F.col('data') > last_load_date)

        colunas = ["cnpj", "data", "endereco","complemento", "bairro",  "municipio","cep", "uf"]

        df_address_adm_consorcio_selected = df_address_adm_consorcio.select(colunas)
        df_bancos_selected = df_bancos.select(colunas)
        df_cooperativas_credito_selected = df_cooperativas_credito.select(colunas)
        df_sociedades_selected = df_sociedades.select(colunas)

        dataframes_selected = [df_address_adm_consorcio_selected, df_bancos_selected, df_cooperativas_credito_selected, df_sociedades_selected]

        dataframes_selected = [
            df.withColumn("number", F.lit(None).cast("integer"))
            .withColumn("date_end", F.lit(None).cast("date"))
            for df in dataframes_selected]

        def unionAll(dataframes):
            return reduce(DataFrame.unionAll, dataframes)

        df_enderecos = unionAll(dataframes_selected).withColumn("address_type", F.lit("SEDE"))

        logger.info("Persistindo o DataFrame de endereços")
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

        logger.info("Persistindo o DataFrame com endereços corrigidos")
        df_tipo_corrigido.persist()

        regex = r"(\d+(?:\.\d+)?)"

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

        logger.info("Persistindo o DataFrame com números de endereços extraídos")
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

        logger.info("Persistindo o DataFrame com endereços sem caracteres especiais")
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

        df_colunas = df_colunas.withColumn("branch_code", F.lit(None))

        df_final = df_colunas.withColumn("postalcode", F.regexp_replace(F.col("postalcode"), "-", ""))

        logger.info("Atualizando date_end para registros que receberam atualização")
        dim_endereco = G.table_to_df('dw_finance', 'dim_endereco', spark)

        joined_df = dim_endereco.join(df_final, dim_endereco["cnpj"] == df_final["cnpj"], "left_outer") \
            .where(dim_endereco["date_end"].isNull())

        update_df = joined_df.select(df_final["date_end"].alias("new_date_end"), dim_endereco["*"])

        dim_endereco_updated = update_df.withColumn("date_end", F.col("new_date_end")).drop("new_date_end")

        max_date_row = df_final.agg(F.max(F.col("registration_date")).alias("max_date")).collect()[0]
        last_date = max_date_row["max_date"]

        logger.info(f"Última data de registro: {last_date}")

        schema = T.StructType([
            T.StructField("load_date", T.DateType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True)])

        data = [(last_date, 'dim_endereco', 'sede')]

        last_date_df = spark.createDataFrame(data, schema)

        config_dim_endereco = db_config("dw_finance", "dim_endereco")

        config_table_load_control = db_config("dw_finance", "dw_load_control")

        logger.info("Salvando os dados na tabela dim_endereco")
        df_final.write.mode("append").format("jdbc").options(
            url=config_dim_endereco["url"],
            dbtable=config_dim_endereco["dbtable"],
            user=config_dim_endereco["user"],
            password=config_dim_endereco["password"],
            driver=config_dim_endereco["driver"]
        ).save()

        dim_endereco_updated.write.mode("overwrite").format("jdbc") \
            .options(
            url=config_dim_endereco["url"],
            dbtable=config_dim_endereco["dbtable"],
            user=config_dim_endereco["user"],
            password=config_dim_endereco["password"],
            driver=config_dim_endereco["driver"]).save()

        logger.info("Salvando os dados na tabela de controle de carga")
        last_date_df.write.mode("append").format("jdbc").options(
            url=config_table_load_control["url"],
            dbtable=config_table_load_control["dbtable"],
            user=config_table_load_control["user"],
            password=config_table_load_control["password"],
            driver=config_table_load_control["driver"]
        ).save()

        logger.info("Finalizando o Spark Session")
        spark.stop()
        logger.info("Processo concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro durante a execução do processo: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dim_endereco_sede_pipeline',
    default_args=default_args,
    description='Pipeline de carga incremental para endereços de sedes',
    schedule_interval=timedelta(days=1),
    catchup=False
)

run_pipeline_task = PythonOperator(
    task_id='etl_dim_endereco_sedes',
    python_callable=etl_dim_endereco_sede,
    dag=dag,
)

# Sensors to wait for the other DAGs
wait_for_sedesBancos = ExternalTaskSensor(
    task_id='wait_for_sedes_bancos',
    external_dag_id='Sedes_bancos', 
    external_task_id='collect_sedes_bancos',  
    mode='poke',
    poke_interval=300,
    timeout=6000,
    dag=dag,
)

wait_for_sedesCooperativas = ExternalTaskSensor(
    task_id='wait_for_sedes_cooperativas',
    external_dag_id='sedes_cooperativas_credito',
    external_task_id='collect_sedes_cooperativas',
    mode='poke',
    poke_interval=300,
    timeout=6000,
    dag=dag,
)

wait_for_sedesSociedades = ExternalTaskSensor(
    task_id='wait_for_sedes_sociedades',
    external_dag_id='sedes_sociedades',
    external_task_id='collect_sedes_sociedades',
    mode='poke',
    poke_interval=300,
    timeout=6000,
    dag=dag,
)

wait_for_sedesAdministradoras = ExternalTaskSensor(
    task_id='wait_for_sedes_administradoras',
    external_dag_id='sedes_administradoras_consorcio',
    external_task_id='collect_sedes_administradoras',
    mode='poke',
    poke_interval=300,
    timeout=6000,
    dag=dag,
)

[wait_for_sedesBancos, wait_for_sedesCooperativas, wait_for_sedesSociedades, wait_for_sedesAdministradoras] >> run_pipeline_task

