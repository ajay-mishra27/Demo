
import pyodbc 
import json

class SQLConnection:
    def __init__(self):
        print("Initializing MySQL connection.")
        self.conn = None
    

    def get_connection(self):
        SERVER = 'velo23.czitegfubxle.us-east-1.rds.amazonaws.com' 
        DATABASE = 'paymentProcessing'
        USERNAME = 'admin'
        PASSWORD = 'Admin123!'
        connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
        self.conn = pyodbc.connect(connectionString) 

    def get_pipeline_config(self):
        results = []
        fresults = []
        cursor = self.conn.cursor()
        cursor.execute("""
                            SELECT TOP (10) [pipeline_config_id]
                ,[pipeline_name]
                ,[json_message]
                ,[created_by]
                ,[created_at]
                ,[updated_by]
                ,[updated_at]
            FROM [paymentProcessing].[dbo].[t_pipeline_config] where pipeline_config_id=1
            """)
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchall()
        for row in result:
            results.append(dict(zip(columns, row)))
        for row in results:
            fresults.append(json.loads(row['json_message']))
        return fresults   

    def get_kafka_messages(self):
        results = []
        fresults = []
        cursor = self.conn.cursor()
        cursor.execute("""
                                    SELECT TOP (10) [id]
                        ,[message]
                        ,[created_at]
                        ,[pipeline_config_id]
                        ,[file_name]
                    FROM [paymentProcessing].[dbo].[t_kafka_raw_dump]

                    where pipeline_config_id=1
            """)
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchall()
        for row in result:
            results.append(dict(zip(columns, row)))
        for row in results:
            fresults.append(json.loads(row['message']))
        return fresults