import utils.data_enrich as data_enrich
import utils.data_transform as data_transform



class DataEnrichment:

    def __init__(self):
        print("initializing Date Enrichment service")
    
    def perform_action(self,action,each_json,action_count,each_tran):
        data_element_1 = ""
        data_element_2 = ""
        target_val = ""
        parameter_length = 10
        data_mask_val = "0"
        direction_is_left = True
        concat_val = " "

        if "data_element_"+str(action_count)+"_1" in each_json:
            data_element_1 = each_json["data_element_"+str(action_count)+"_1"]
            target_val = data_element_1
            data_element_1 = "" if data_element_1 not in each_tran else each_tran[data_element_1]
            
        if "data_element_"+str(action_count)+"_2" in each_json and each_json["data_element_"+str(action_count)+"_2"] != None:
            data_element_2 = each_json["data_element_"+str(action_count)+"_2"]
            data_element_2 = "" if data_element_2 not in each_tran else each_tran[data_element_2]

        if "target_element_"+str(action_count) in each_json and each_json["target_element_"+str(action_count)] != None:
            target_val = each_json["target_element_"+str(action_count)]

        if "action_parameter_"+str(action_count) in each_json:
            if each_json["action_parameter_"+str(action_count)] != None:
                action_parameter = each_json["action_parameter_"+str(action_count)]  
                #val = lambda val : temp_val if val == "" else val  
                direction_is_left = True if "direction" not in action_parameter else True if action_parameter['direction'] == 'left' else False
                data_mask_val = "0" if "value_to_add" not in action_parameter else action_parameter['value_to_add']
                parameter_length = 10 if "length" not in action_parameter else action_parameter['length']
                concat_val =  " " if "concat_val" not in action_parameter else action_parameter['concat_val']
        
        match action:
            case "Padding":
                value = data_enrich.str_padding(data_element_1,parameter_length,direction_is_left,data_mask_val)
                print(value)
            case "trim":
                value = data_enrich.str_trim(data_element_1)
            case "Concatenation":
                value = data_enrich.str_concat(data_element_1,data_element_2,concat_val)
            case "substring":
                value = data_enrich.str_sub_strig(data_element_1,0,2)
            case "caseconversion":
                value = data_enrich.str_case_conversion(each_tran)
            case "mask":
                value = data_enrich.str_mask(data_element_1,data_mask_val)
            case "typeconversion":
                value = data_enrich.type_conversion(data_element_1)
            case _:
                raise(f"invalid action came {action}.")
        each_tran[target_val] = value
        return each_tran
            
    def perform_data_enrich(self,each_trans,json_arryay):
        for each_json in json_arryay:
            process = True
            action_count = 1
            while process ==True:
                action_str = 'action_'+str(action_count)
                if action_str in each_json:
                    action = each_json[action_str]
                    each_trans = self.perform_action(action,each_json,action_count,each_trans)
                    action_count = action_count + 1
                else:
                    process = False
        
        return each_trans
                
            
    
    def perform_data_enrichment(self,data_frame,input_json):
        
        print("Started data enricment process")
        final_json_array = []
        index = 0
        for each_trans in data_frame:
            final_tran = {}
            print(f"Strted for the data enrichment for row {index}")
            final_tran = self.perform_data_enrich(each_trans,input_json)
            final_json_array.append(final_tran)
            index = index +1
        print("Ended data enricment process")
        return final_json_array
    

    def perform_data_trans(self,each_trans,json_arryay):
        for each_json in json_arryay:
            process = True
            action_count = 1
            val = ""
            opr_str = ""
            while process ==True:
                action_str = 'condition_'+str(action_count)
                if action_str in each_json:
                    action = each_json[action_str]
                    if val != "":
                        opr_str = each_json['Operator_'+str(action_count-1)]
                    temp_val = self.perform_trans_action(action,each_json,action_count,each_trans)
                    val = lambda val : temp_val if val == "" else val
                    if opr_str != "":
                        opr_str = opr_str.lower()
                        match opr_str:
                            case "and":
                                val = val and temp_val
                            case "or":
                                val = val or temp_val
                            case _:
                                raise(f"invalid operation came {opr_str}.")
                    action_count = action_count + 1
                else:
                    process = False
            print(val)
            if val == True:
                action = each_json['action']
                action_array = action.split(" ")
                each_trans[action_array[1]] = action_array[2]

        
        return each_trans
    
    def perform_trans_action(self,action,each_json,action_count,each_tran):
        data_element = ""
        data_value = ""
        value = False

        if "data_element_"+str(action_count)+"_1" in each_json:
            data_element = each_json["data_element_"+str(action_count)+"_1"]
            data_element = "" if data_element not in each_tran else each_tran[data_element]
        
            
        if "value_"+str(action_count) in each_json:
            data_value = each_json["value_"+str(action_count)]
            data_value = "" if data_value not in each_tran else each_tran[data_value]


        match action:
            case "is_empty":
                value,data_element = data_enrich.is_str_empty(data_element)
            case "is_not_empty":
                value,data_element = data_enrich.is_str_empty(data_element)
            case "Equal_to":
                value = data_transform.is_string_eq(data_element,data_value)
            case "not_equal":
                value = data_transform.is_string_eq(data_element,data_value)
            case "starts_with":
                value = data_transform.is_string_eq(data_element,data_value)
            case "not_starts_with":
                value = data_transform.is_string_eq(data_element,data_value)    
            case "contains":
                value = data_transform.is_str_contains(data_element,data_value)
            case "not_contains":
                value = data_transform.is_str_contains(data_element,data_value)
            case "is_substring":
                value = data_transform.is_string_eq(data_element,data_value)
            case "is_not_substring":
                value = data_transform.is_string_eq(data_element,data_value)
            case _:
                raise(f"invalid action came {action}.")
        if "not" in action:
                value = lambda value : False if value == True else True
        return value
    
    
    def perform_data_transform(self,data_frame,input_json):

        print("Started data transform process")
        final_json_array = []
        index = 0
        for each_trans in data_frame:
            final_tran = {}
            print(f"Strted for the data transform for row {index}")
            final_tran = self.perform_data_trans(each_trans,input_json)
            final_json_array.append(final_tran)
            index = index +1
        print("Ended data transform process")
        return final_json_array