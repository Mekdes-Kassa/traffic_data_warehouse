from airflow import DAG
#from airflow import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from src.extract import CSVDataReader
from src.load import DataLoadToPostgres
from src.transform import dbt_transform


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data():
    csv_reader = CSVDataReader('/home/hp/Documents/Traffic/egz/Data/20181024_d1_0830_0900.csv')
    csv_reader.read_data()
    df_track = csv_reader.df_track
    df_Trajectory = csv_reader.df_Trajectory
    return df_track, df_Trajectory

dbt_project_dir ='/home/hp/Documents/Traffic/egz/dbt_data_transforming'

def load_data():
    loader = DataLoadToPostgres('/home/hp/Documents/Traffic/egz/Data/20181024_d1_0830_0900.csv')
    loader.read_and_load_data()

    conn = loader.connect_to_postgres('Data', 'postgres', '1234', 'localhost', '5432')
    loader.create_schema(conn, 'Data')
    loader.create_tables(conn, 'Data')

    engine_str = 'postgresql://postgres:1234@localhost:5432/Data'
    loader.load_to_postgres(engine_str, 'Data')

    loader.close_connection(conn)

# Define the DAG
dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='A simple DAG for data processing',
    schedule_interval=timedelta(days=1)
)

# Define tasks
#start_task = DummyOperator(task_id='start', dag=dag)
extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
dbt_transform_task = PythonOperator(task_id='dbt_transform',python_callable=dbt_transform,op_kwargs={'dbt_project_dir':dbt_project_dir},dag=dag)
load_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)
#end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
extract_task >> load_task >> dbt_transform_task 
