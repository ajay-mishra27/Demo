import sys
from services.data_enrichment_service import DataEnrichment

if __name__ == "__main__":
    try:
        print("MT103 file Conversion started")
        process = DataEnrichment()
        input_json = [
    {
        "id": 1,
        "name": "Sample_config_7f793b33-fcbf-4510-8844-2029e6f82388",
        "createdAt": "2023-12-09 16:22:00.360000",
        "updatedAt": "2023-12-09 16:22:00.360000",
        "config": {
            "projectid": "1",
            "pipelineName": "Sample_config_7f793b33-fcbf-4510-8844-2029e6f82388",
            "configuration": [
                {
                    "source_kde": {
                        "source_kde_name": "Source_KDE_1",
                        "rows": [
                            {
                                "element_tag": "First_name",
                                "element_tag_value": "Eric",
                                "element_tag_group": "Name"
                            },
                            {
                                "element_tag": "Middle_name",
                                "element_tag_value": "Church",
                                "element_tag_group": "Name"
                            },
                            {
                                "element_tag": "Last_name",
                                "element_tag_value": "Hill",
                                "element_tag_group": "Name"
                            }
                        ],
                        "Message_variants": [
                            {
                                "variant_name": "Message_Varient_1",
                                "pipeline_operators": [
                                    {
                                        "enricher": {
                                            "enricher_id": "12",
                                            "enricher_name": "enrich_12",
                                            "rows": [
                                                {
                                                    "data_element_1_1": "firstName",
                                                    "data_element_1_2": "lasstName",
                                                    "action_1": "concat",
                                                    "action_parameter_1": "fullname"
                                                },
                                                {
                                                    "data_element_1_1": "accountNum",
                                                    "data_element_1_2": None,
                                                    "action_1": "mask",
                                                    "action_parameter_1": None,
                                                    "data_mask_val_1":"X"

                                    
                                                },
                                                {
                                                    "data_element_1_1": "type",
                                                    "data_element_1_2": None,
                                                    "action_1": "trim",
                                                    "action_parameter_1": None,
                                                    "data_mask_val_1":"X"

                                    
                                                }
                                            ]
                                        },
                                        "transformer": {
                                            "transformer_id": "12",
                                            "transformer_name": "transform_12",
                                            "rows": [
                                                {
                                                    "data_element_1_1": "sendersABA",
                                                    "condition_1": "equal",
                                                    "value_1": "CITIABA",
                                                    "Operator_1": "and",
                                                    "data_element_2_1": "receiversABA",
                                                    "condition_2": "equal",
                                                    "value_2": "CITIABA",
                                                    "action": "set message null"
                                                },
                                                {
                                                    "data_element_1_1": "type",
                                                    "condition_1": "equal",
                                                    "value_1": "wire",
                                                    "action": "set type ach"
                                                }
                                            ]
                                        }
                                    }
                                ],
                                "target_kdes": [
                                    {
                                        "target_kde": "asdf"
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
    }
]
        json_arryay_enrich = []
        json_arryay_transfarmeres = []
        dataframes = [{"firstName":"Test","lasstName":"val","accountNum":"232312312","type":"wire  ","sendersABA":"CITIABA","receiversABA":"CITIABA","rountingVal":"4343433","message":"HI"},
                      {"firstName":"Bhanu","lasstName":"Oleti","accountNum":"3432432","type":"  posting","sendersABA":"424","receiversABA":"CITIABA","rountingVal":"4343433","message":"HI"}]
        print(dataframes)
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
        dataframes = process.perform_data_enrichment(dataframes,json_arryay_enrich)
        print("Date after enrichment")
        print(dataframes)
        dataframes = process.perform_data_transform(dataframes,json_arryay_transfarmeres)
        print(dataframes)
        print("MT103 File conversion Ended")
    except Exception as exp:
        print("Excetion")
        raise(exp)