import sys
from services.data_enrichment_service import DataEnrichment  
from utils.sql_connection import SQLConnection
import json

if __name__ == "__main__":
    try:
        print("MT103 file Conversion started")
        process = DataEnrichment()
        sql_class = SQLConnection()
        sql_class.get_connection()
        input_json = sql_class.get_pipeline_config()
        dataframes = sql_class.get_kafka_messages()
        json_arryay_enrich = []
        json_arryay_transfarmeres = []
        for each_json in input_json:
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
        print(dataframes)
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