
import data_enrich as data_enrich


#Checks both strings are equal or not
def is_string_eq(input_string_1,input_string_2,is_casesenstive = False):
    is_empty ,input_string_1 = data_enrich.is_str_empty(input_string_1)
    is_empty ,input_string_2 = data_enrich.is_str_empty(input_string_2)
    if is_casesenstive == True:
        return input_string_1 == input_string_2
    else:
        return data_enrich.str_case_conversion(input_string_1,"lower") == data_enrich.str_case_conversion(input_string_2,"lower")

#String starts with    
def str_start_with(input_string,start_with,is_casesenstive = False):
    is_empty ,input_string = data_enrich.is_str_empty(input_string)
    if is_casesenstive == False:
        input_string =  data_enrich.str_case_conversion(input_string,"lower")
        start_with = data_enrich.str_case_conversion(start_with,"lower")

    return input_string.startswith(start_with)