
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

    def get_pipeline_config(self,row_id):
        results = []
        fresults = []
        cursor = self.conn.cursor()
        cursor.execute(f"""
                                SELECT [pipeline_config_id]
            ,[pipeline_name]
            ,[json_message]
            ,[created_by]
            ,[created_at]
            ,[updated_by]
            ,[updated_at]
        FROM [paymentProcessing].[dbo].[t_pipeline_config] 
        where pipeline_config_id=(select pipeline_config_id from paymentProcessing.dbo.t_kafka_raw_dump tkrd where id={row_id})
                """)
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchall()
        for row in result:
            results.append(dict(zip(columns, row)))
        for row in results:
            fresults.append(json.loads(row['json_message']))
        cursor.close()
        return fresults
           

    def get_kafka_messages(self,row_id):
        results = []
        cursor = self.conn.cursor()
        cursor.execute(f"""
                                    SELECT [id]
                        ,[message]
                        ,[created_at]
                        ,[pipeline_config_id]
                        ,[file_name]
                    FROM [paymentProcessing].[dbo].[t_kafka_raw_dump]
                    where id={row_id}
            """)
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchall()
        for row in result:
            results.append(dict(zip(columns, row)))
        cursor.close()
        return results
    
    def close_db_connection(self):
        self.conn.close()


    def execute_query(self,query):
        cursor =  self.conn.cursor()
        if query.startswith("select"):
            cursor.execute(query)
            rows = cursor.fetchall()
        else:
            rows = []
            cursor.execute(query)
            # Get the id of the executed insert query
            new_id_query = "SELECT SCOPE_IDENTITY();"
            cursor.execute(new_id_query)
            row_id = cursor.fetchone()[0]
            rows.append(str(row_id))
            cursor.execute("Commit;")
        return rows
        