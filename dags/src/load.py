import psycopg2
from sqlalchemy import create_engine
from src.extract import CSVDataReader

class DataLoadToPostgres:
    def __init__(self, csv_path):
        self.csv_reader = CSVDataReader(csv_path)
        self.df_track = None
        self.df_Trajectory = None
    
    def read_and_load_data(self):
        self.csv_reader.read_data()
        self.df_track = self.csv_reader.df_track
        self.df_Trajectory = self.csv_reader.df_Trajectory
    
    def connect_to_postgres(self, dbname, user, password, host, port):
        connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        conn = psycopg2.connect(**connection_params)
        return conn
    
    def create_schema(self, conn, schema_name):
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        conn.commit()
        cursor.close()
    
    def create_tables(self, conn, schema_name):
        cursor = conn.cursor()
        create_track_table_query = """
            CREATE TABLE IF NOT EXISTS {}.track (
                track_id INT,
                type VARCHAR(50),
                avg_speed FLOAT,
                traveled_d FLOAT
            )
        """.format(schema_name)

        create_trajectory_table_query = """
            CREATE TABLE IF NOT EXISTS {}.trajectory (
                track_id INT,
                lat FLOAT,
                lon FLOAT,
                speed FLOAT,
                lon_acc FLOAT,
                lat_acc FLOAT,
                time FLOAT
            )
        """.format(schema_name)

        cursor.execute(create_track_table_query)
        cursor.execute(create_trajectory_table_query)
        conn.commit()
        cursor.close()
    
    def load_to_postgres(self, engine, schema_name):
        engine = create_engine(engine)
        self.df_track.to_sql('track', engine, schema=schema_name, if_exists='replace', index=False)
        self.df_Trajectory.to_sql('trajectory', engine, schema=schema_name, if_exists='replace', index=False)
    
    def close_connection(self, conn):
        conn.close()