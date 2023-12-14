import data_enrich as data_enrich
import os

#String contains    
def str_contains(input_string,string_to_check,is_casesenstive = False):
    is_empty ,input_string = data_enrich.is_str_empty(input_string)
    if is_casesenstive == False:
        input_string =  data_enrich.str_case_conversion(input_string,"lower")
        string_to_check = data_enrich.str_case_conversion(string_to_check,"lower")

    return string_to_check in input_string


#check if file contains
def file_exists(input_file_name,path):
    file = os.path.join(path,input_file_name)
    return os.path.exists(file)

#array contains string
def arrray_contains_string(input_array,string_to_check):
    return string_to_check in input_array
