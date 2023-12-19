import sys
from services.data_enrichment_service import DataEnrichment  
import pyodbc 
import json

if __name__ == "__main__":
    try:
        print("MT103 file Conversion started")
        process = DataEnrichment()
        SERVER = 'velo23.czitegfubxle.us-east-1.rds.amazonaws.com' 
        DATABASE = 'paymentProcessing'
        USERNAME = 'admin'
        PASSWORD = 'Admin123!'
        connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
        conn = pyodbc.connect(connectionString) 
        
        try:
            cursor = conn.cursor()
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
            cursor.execute("""
                                    SELECT TOP (10) [id]
                        ,[message]
                        ,[created_at]
                        ,[pipeline_config_id]
                        ,[file_name]
                    FROM [paymentProcessing].[dbo].[t_kafka_raw_dump]

                    where pipeline_config_id=1
            """)
            columns1 = [column[0] for column in cursor.description]
            result1 = cursor.fetchall()
        finally:
            conn.close()
        input_json = []
        results=[]
        dataframes = []
        dataframe = []
        json_arryay_enrich = []
        json_arryay_transfarmeres = []
        for row in result:
            results.append(dict(zip(columns, row)))
        for row in result1:
            dataframe.append(dict(zip(columns1, row)))
        for row in results:
            input_json.append(json.loads(row['json_message']))
        for row in dataframe:
            dataframes.append(json.loads(row['message']))
        for each_json in input_json:
            print(each_json["pipelineName"])
            configuration_json = each_json['config']['configuration']
            for each_config_json in configuration_json:
                message_variants_json = each_config_json['source_kde']['Message_variants']
                for message_variants_each_json in message_variants_json:
                    pipeline_operators = message_variants_each_json['pipeline_operators']
                    for pipeline_operators_json in pipeline_operators:
                        for each_json in pipeline_operators_json['enricher']['rows']:
                            json_arryay_enrich.append(each_json)
                        for each_json in pipeline_operators_json['transformer']['rows']:
                            json_arryay_transfarmeres.append(each_json)
        dataframes = process.perform_data_enrichment(dataframes,json_arryay_enrich)
        print("Date after enrichment")
        print(dataframes)
        dataframes = process.perform_data_transform(dataframes,json_arryay_transfarmeres)
        print(dataframes)
        with open("test.txt",'w') as file:
            json.dump(dataframes,file,ensure_ascii=False)
        print("MT103 File conversion Ended") 
    except Exception as exp:
        print("Excetion")
        raise(exp)