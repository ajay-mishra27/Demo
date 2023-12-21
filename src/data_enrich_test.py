import sys
from services.data_enrichment_service import DataEnrichment 
from utils.sql_connection import SQLConnection 
import pyodbc 
import json

def transform_enhance_data(row_id, simulation=False):
    print("File enrichment started")
    try:
        print("File transaformation started")
        process = DataEnrichment()
        sqlConnection = SQLConnection()
        dataframes = []
        sqlConnection.get_connection()
        results = sqlConnection.get_kafka_messages(row_id)
        input_json = sqlConnection.get_pipeline_config(row_id)
        pipeline_id = 0
        for row in results:
            dataframes.append(json.loads(row['message']))
            pipeline_id = row['pipeline_config_id']

        
        
        print(dataframes)
        json_arryay_enrich = []
        json_arryay_transfarmeres = []
        
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
        dump_row_id = ""
        for eachJson in dataframes:
            query = f'insert into dbo.t_process_message_dump (id, message, process_status, pipeline_config_id) values (\'{row_id}\',\'{json.dumps(eachJson)}\',\'success\',\'{pipeline_id}\');'
            dump_row_id = sqlConnection.execute_query(query)
        # with open("test.txt",'w') as file:
        #     json.dump(dataframes,file,ensure_ascii=False)
        print(f"File Transformation Ended {dump_row_id}") 
        sqlConnection.close_db_connection()
        return(row_id)
        
    except Exception as exp:
        print("Excetion")
        #print(exp.message)
        raise(exp)
    
if __name__ == "__main__":
    transform_enhance_data(42)
    
