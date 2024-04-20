import psycopg2
from config import *

def connect_postgres(user, password, host, port, database):
    try:
        connection = psycopg2.connect(user=postgres_user,
                                      password=postgres_password,
                                      host=postgres_host,
                                      port=postgres_port,
                                      database=postgres_database)
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Couldn't connect to the PostgreSQL:", error)
        return None



connection = connect_postgres(postgres_user,postgres_password,postgres_host,postgres_port,postgres_database) 

def create_table_postgres(table_name, schema):
    try:
 
        cursor = connection.cursor()
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})" # Checking if table already exits before creatring one. 
        cursor.execute(create_table_sql) #using cursor functions to run sql queries 
        connection.commit()
        print(f"{table_name}' is created successfully.")
        return cursor
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return None

def insert_csv_to_postgres(cursor, csv_file, table_name):
    try:
        with open(csv_file, 'r') as f:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f) #using cursor functions to run sql queries 

        cursor.connection.commit()
        print(f"Data from '{csv_file}' inserted into the table '{table_name}' successfully.")
    except Exception as e:
        print(f"error inserting data to the posgres {table_name}: {e}")


cursor = create_table_postgres("tweets_table", schema) # This function creates a table if it doesnot exit and returns the cursor 
insert_csv_to_postgres(cursor, "./data/tweets_after_processing.csv", "tweets_table") # using the cursor returned, this function inserts the csv data to the posgres table that we have created


cursor.close()
cursor.connection.close() #Closing the connections once the transaction is done for effient utilization of resources .