from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import zipfile
import os

#DAG arguments
default_args = {
    'owner': 'Daiane',
    'start_date': days_ago(0),
    'email': ['daianemacedo001@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



def unzip_file(**kwargs):
    # Caminho do arquivo ZIP a ser descompactado
    zip_file_path = "/home/airflow/final_project_ibm/tolldata.tgz"

    # Pasta de destino para extrair os arquivos
    extract_to_folder = "/aiflow/final_project_ibm"

    # Cria a pasta de destino, se ainda não existir
    os.makedirs(extract_to_folder, exist_ok=True)

    # Descompacta o arquivo ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)

def extract_data_from_csv(**kwargs):
    csv_file_path = "/home/airflow/final_project_ibm/vehicle-data.csv"

    output_file_path = "/home/airflow/final_project_ibm/csv_data.csv"

    #Extrai os campos especificados 
    data = pd.read_csv(csv_file_path)
    selected_data = data[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]

    selected_data.to_csv(output_file_path, index=False)


def extract_data_from_tsv(**kwargs):
    tsv_file_path = "home/airflow/final_project_ibm/tollplaza-data.tsv"
    output_file_path = "home/airflow/final_project_ibm/tsv_data.csv"

    #extrai campos especificados em um novo csv
    data = pd.read_csv(tsv_file_path, sep='\t')
    selected_data = data[['Number of axles', 'Tollplaza id', 'Tollplaza code']]

    selected_data.to_csv(output_file_path, index=False)


def extract_data_from_fixed_width(**kwargs):
    fixed_width_file_path = "home/airflow/final_project_ibm/payment-data.txt"

    output_file_path = "home/airflow/final_project_ibm/fixed_width_data.csv"

    col_widths = col_widths = [None, None, None, None, None, None, None, None, None, 3, 5]

    data = pd.read_fwf(fixed_width_file_path, widths=col_widths, header=None, names=['Type of Payment code', 'Vehicle Code'])

    data.to_csv(output_file_path, index=False)

def transform_data(**kwargs):
    input_file_path = "/home/airflow/final_project_ibm/extracted_data.csv"
    output_file_path = "/home/airflow/final_project_ibm/transformed_data.csv"

    data = pd.read_csv(input_file_path)

    data['Vehicle type'] = data['Vehicle type'].str.upper()

    data.to_csv(output_file_path, index=False)



dag = DAG(
    'ETL_toll_data',
    default_args= default_args,
    description = '	Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1)
)

unzip_data = PythonOperator(
    task_id='descompactar_arquivo',
    python_callable=unzip_file,
    provide_context=True,
    dag=dag,
)

extract_data_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    provide_context=True,
    dag=dag,
)

extract_data_tsv = PythonOperator(
    task_id = 'extract_data_from_tsv',
    python_callable = extract_data_from_csv,
    provide_context = True,
    dag = dag
)

extract_data_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    provide_context=True,
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," /home/airflow/final_project_ibm/csv_data.csv /home/airflow/final_project_ibm/tsv_data.csv /home/airflow/final_project_ibm/fixed_width_data.csv | awk \'BEGIN {FS=","; OFS=","} {print $1, $2, $3, $4, $5, $6, $7, $8, $9}\' > /home/airflow/final_project_ibm/extracted_data.csv',
    dag=dag,
)


transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

unzip_data >> extract_data_csv >> extract_data_tsv >> extract_data_fixed_width >> consolidate_data >> transform_data_task

