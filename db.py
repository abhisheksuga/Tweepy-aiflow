import json
import pandas as pd
from pymongo import MongoClient
import psycopg2
from sqlalchemy import create_engine   #alternate option to use, instead of psycopg2

class Database:
    def __init__(self, config_file):
        self.config_file = config_file
        self.load_config()

    def load_config(self):
        print("Attempting to load config file from:", self.config_file)
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)
        print("Config file loaded successfully.")

    def connect_mongodb(self):
        mongo_uri = self.config.get('mongo_uri')
        db_name = self.config.get('mongo_db_name')
        collection_name = self.config.get('mongo_collection_name')
        try:
            self.mongo_client = MongoClient(mongo_uri)
            self.mongo_db = self.mongo_client[db_name]
            self.mongo_collection = self.mongo_db[collection_name]
            print("Connected to MongoDB successfully.")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")

    def fetch_data_mongodb(self):
        try:
            data = self.mongo_collection.find({}, {'_id': 0})
            data_dict = [d for d in data]
            return data_dict
        except Exception as e:
            print(f"Error fetching data from MongoDB: {e}")
            return None

    def connect_postgres(self):
        postgres_user = self.config.get('postgres_user')
        postgres_password = self.config.get('postgres_password')
        postgres_host = self.config.get('postgres_host')
        postgres_port = self.config.get('postgres_port')
        postgres_database = self.config.get('postgres_database')
        try:
            self.postgres_connection = psycopg2.connect(user=postgres_user,
                                                        password=postgres_password,
                                                        host=postgres_host,
                                                        port=postgres_port,
                                                        database=postgres_database)
            print("Connected to PostgreSQL successfully.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")

    def create_table_postgres(self, table_name):
        schema = self.config.get('postgres_schema')
        if not schema:
            print("Error: PostgreSQL schema not found in config file.")
            return None
        
        try:
            cursor = self.postgres_connection.cursor()
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join([f'{col} {data_type}' for col, data_type in schema.items()])})"
            cursor.execute(create_table_sql)
            self.postgres_connection.commit()
            print(f"Table '{table_name}' created successfully.")
            cursor.close()
            return self.postgres_connection
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            return None

    def insert_data_postgres(self, csv_file, table_name):
        try:
            cursor = self.postgres_connection.cursor()
            with open(csv_file, 'r') as f:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

            self.postgres_connection.commit()
            print(f"Data from '{csv_file}' inserted into the table '{table_name}' successfully.")
            cursor.close()
        except Exception as e:
            print(f"Error inserting data into PostgreSQL table {table_name}: {e}")

    def close_connections(self):
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()
        if hasattr(self, 'postgres_connection'):
            self.postgres_connection.close()


db = Database(config_file = 'config.json')
# # db.connect_mongodb()
# # mongo_data = db.fetch_data_mongodb()

# db.connect_postgres()
# postgres_table_name = 'tweets_testing'
# db.create_table_postgres(table_name=postgres_table_name)

# # csv_file = './data/processed_data.csv'
# # db.insert_data_postgres(csv_file=csv_file, table_name=postgres_table_name)

# db.close_connections()
