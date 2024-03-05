from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from src.extract_data_source import CSVDataReader
from src.load_data import DataLoadToPostgres

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    
}

# Instantiate a DAG with the defined default_args
dag = DAG(
    'traffic_flow',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),  # Set the schedule interval to run daily
)


def task_a_function():
    csv_reader = CSVDataReader('/home/hp/Documents/DAta_engineering/traffic_data_warehouse/Data/20181024_d1_0830_0900.csv')
    csv_reader.read_data()
    df_track = csv_reader.df_track
    df_Trajectory = csv_reader.df_Trajectory
    return df_track, df_Trajectory
    

task_a_task = PythonOperator(
    task_id='Extract',
    python_callable=task_a_function,
    dag=dag,
)


def task_b_function():
    loader = DataLoadToPostgres('/home/hp/Documents/DAta_engineering/traffic_data_warehouse/Data/20181024_d1_0830_0900.csv')
    loader.read_and_load_data()

    conn = loader.connect_to_postgres('Data', 'postgres', '1234', 'localhost', '5432')
    loader.create_schema(conn, 'Data')
    loader.create_tables(conn, 'Data')

    engine_str = 'postgresql://postgres:1234@localhost:5432/Data'
    loader.load_to_postgres(engine_str, 'Data')

    loader.close_connection(conn)


task_b_task = PythonOperator(
    task_id='load',
    python_callable=task_b_function,
    dag=dag,
)
def task_c_function():
    print("transforming data")




# Define the order and dependencies between tasks in the DAG
task_a_task >> task_b_task 