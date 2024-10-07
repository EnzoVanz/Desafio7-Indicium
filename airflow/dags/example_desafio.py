from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd
import time

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

def read_orders_to_csv():
    conn = sqlite3.connect('/opt/data/Northwind_small.sqlite')
    df = pd.read_sql_query("SELECT * FROM 'Order'", conn)
    conn.close()
    df.to_csv('output_orders.csv')
    time.sleep(5)

def count():
    conn = sqlite3.connect('/opt/data/Northwind_small.sqlite')
    df_orderdetail = pd.read_sql_query("SELECT * FROM 'OrderDetail'", conn)
    conn.close()

    df_orders = pd.read_csv('/opt/airflow/output_orders.csv')
    df_merged = pd.merge(df_orders, df_orderdetail, left_on='Id', right_on='OrderId', how='inner')

    rio_count = df_merged[df_merged['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()

    with open('count.txt', 'w') as f:
        f.write(str(rio_count))


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 6),
    catchup=True,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    read_orders_task = PythonOperator(
        task_id='read_orders',
        python_callable=read_orders_to_csv,
    )

    count_task = PythonOperator(
        task_id='count_quantity_rio',
        python_callable=count,
    )

    read_orders_task >> count_task >> export_final_output