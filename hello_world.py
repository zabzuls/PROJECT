# komponen pendukung
import datetime as dt
import sys

PATH_TO_PYTHON_BINARY = sys.executable

# komponen airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator

# fungsi
def print_world() :
    print('world')
    
# atribut dag
default_args = {
    'owner':'UTDI',
    'start_date':dt.datetime(2022,12,16),
    'retries':1,
    'retry_delay':dt.timedelta(minutes=5)}

# operator dag
with DAG('hello_world',
         default_args=default_args,
         schedule='0 * * * *',
        ) as dag :
        print_hello=BashOperator(task_id='print_hello',
        bash_command='echo "hello"')
        sleep=BashOperator(task_id='sleep', bash_command=
        'sleep 5')
        print_world=ExternalPythonOperator(task_id=
        'print_world', python=PATH_TO_PYTHON_BINARY,
        python_callable=print_world,)
        
# eksekusi dag
print_hello >> sleep >> print_world