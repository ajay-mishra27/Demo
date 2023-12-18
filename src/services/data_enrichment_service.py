import utils.data_enrich as data_enrich
import utils.data_transform as data_transform



class DataEnrichment:

    def __init__(self):
        print("initializing Date Enrichment service")
    
    def perform_action(self,action,each_json,action_count,each_tran):
        data_element_1 = ""
        data_element_2 = ""
        action_parameter = ""
        parameter_length = 10
        operator_1 = "" 
        data_mask_val = "0"
        

        if "data_element_"+str(action_count)+"_1" in each_json:
            data_element_1 = each_json["data_element_"+str(action_count)+"_1"]
            action_parameter = data_element_1
            data_element_1 = each_tran[data_element_1]
            
        if "data_element_"+str(action_count)+"_2" in each_json and each_json["data_element_"+str(action_count)+"_2"] != None:
            data_element_2 = each_json["data_element_"+str(action_count)+"_2"]
            data_element_2 = each_tran[data_element_2]

        if "action_parameter_"+str(action_count) in each_json:
            if each_json["action_parameter_"+str(action_count)] != None:
                action_parameter = each_json["action_parameter_"+str(action_count)]    
            
        if "parameter_length_"+str(action_count) in each_json and each_json["parameter_length_"+str(action_count)] != None:
            parameter_length = int(each_json["parameter_length_"+str(action_count)])

        if "data_mask_val"+str(action_count) in each_json and each_json["data_mask_val"+str(action_count)] != None:
            data_mask_val = int(each_json["data_mask_val"+str(action_count)])

        match action:
            case "padding":
                value = data_enrich.str_padding(data_element_1,parameter_length)
            case "trim":
                value = data_enrich.str_trim(data_element_1)
            case "concat":
                value = data_enrich.str_concat(data_element_1,data_element_2)
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
        each_tran[action_parameter] = value
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
            data_element = each_tran[data_element]
        
            
        if "value_"+str(action_count) in each_json:
            data_value = each_json["value_"+str(action_count)]


        match action:
            case "is_empty":
                value,data_element = data_enrich.is_str_empty(data_element)
            case "is_not_empty":
                value,data_element = data_enrich.is_str_empty(data_element)
            case "equal":
                value = data_transform.is_string_eq(data_element,data_value)
            case "not_equal":
                value = data_transform.is_string_eq(data_element,data_value)
            case "starts_with":
                value = data_transform.is_string_eq(data_element,data_value)
            case "not_starts_with":
                value = data_transform.is_string_eq(data_element,data_value)    
            case "contains":
                value = data_transform.is_string_eq(data_element,data_value)
            case "not_contains":
                value = data_transform.is_string_eq(data_element,data_value)
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