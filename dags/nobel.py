from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

def extract_female_laureates(**kwargs):
    """Extraer datos de laureadas femeninas desde la API de Nobel y mostrarlos."""
    url = "https://aapi.nobelprize.org/v1/laureate.json"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data['laureates'])
        
        df_females = df[df['gender'] == 'female']
        print(df_females)
        
        df_females.to_csv('/tmp/female_laureates.csv', index=False)
    else:
        raise Exception(f"Error al obtener datos: {response.status_code}")

def consume_female_laureates(**kwargs):
    """Leer y procesar datos de laureadas desde el archivo CSV."""
    df_females = pd.read_csv('/tmp/female_laureates.csv')
    print("Datos de laureadas femeninas procesados:")
    print(df_females)

def handle_error(**kwargs):
    """Manejar el error y notificar."""
    print("Ocurrió un error en la extracción de datos.")
    raise Exception("Error en la tarea de extracción.")

# Definir DAG y tareas
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=0.1),
    'start_date': datetime(2024, 10, 17),
}

with DAG('nobel_female_laureates_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract_task = PythonOperator(
        task_id='extract_female_laureates',
        python_callable=extract_female_laureates,
        provide_context=True
    )

    error_handling_task = PythonOperator(
        task_id='handle_error',
        python_callable=handle_error,
        trigger_rule='one_failed',
        provide_context=True
    )

    consume_task = PythonOperator(
        task_id='consume_female_laureates',
        python_callable=consume_female_laureates,
        provide_context=True
    )

    email_task = EmailOperator(
        task_id='send_email',
        to='natayadev@gmail.com',
        subject='Error en el DAG de Nobel',
        html_content='Ocurrió un error durante la ejecución de la tarea de extracción de datos.',
        trigger_rule='one_failed'
    )

    echo_task = BashOperator(
        task_id='echo_task',
        bash_command='echo "Esta es una tarea de Bash que se está ejecutando."'
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='ls'
    )

    # Definir el flujo de tareas
    extract_task >> [consume_task, error_handling_task]
    consume_task >> echo_task >> bash_task
    error_handling_task >> email_task