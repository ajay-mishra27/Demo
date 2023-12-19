
def is_str_empty(input_string):
    if input_string == None:
        input_string = "" 
    if str(input_string) == "":
        return True,""
    else:
        return False,str(input_string)
#validate the given string with length
#return length difference and is max length
def length_str(input_string,max_length):
    is_empty,input_string = is_str_empty(input_string)
    print(type(max_length))
    if len(input_string) == max_length:
        return True,0
    else:
        return False,max_length - len(input_string)

#check with string length and will do the padding based left or right padding and with defaut value
def str_padding(input_string,max_length,is_left_padding_truncate = False,padding_value = "0"):

    is_empty,input_string =  is_str_empty(input_string)
    print(input_string)
    is_padding_require,len_diff = length_str(input_string,max_length)
    len_diff = max_length - len(input_string)
    if is_padding_require == False:
        if len_diff < 0:
            if is_left_padding_truncate == True:
                return input_string[len(input_string) - max_length:len(input_string)]
            else:
                return input_string[0:max_length]
        else:
             if is_left_padding_truncate == True:
                return ((padding_value * (max_length - len(input_string))) + input_string)
             else:
                return (input_string + (padding_value * (max_length - len(input_string))))
    else:
        return input_string
    
#This function used to trim based on left or right or full
def str_trim(input_string,type = "full"):
    is_empty,input_string = is_str_empty(input_string)
    if type == "left":
        return input_string.lstrip()
    elif type == "right":
        return input_string.rstrip()
    else:
        return input_string.strip()

#Concatenate two strings
def str_concat(input_string_1,input_string_2,concat_val=" "):
    
    is_empty,input_string_1 =  is_str_empty(input_string_1)
    is_empty,input_string_2 =  is_str_empty(input_string_2)

    return input_string_1 +concat_val+ input_string_2

#Get the sub string with start and end position
def str_sub_strig(input_string,start_position,end_position):
    is_empty,input_string = is_str_empty(input_string)
    if is_empty == True:
        raise("Please provide valid string")
    length_str = len(input_string)
    if length_str > start_position and length_str > end_position and end_position > start_position:
        return input_string[start_position:end_position]
    else:
        raise("Please provide valid string")

#convert the given string either lower or upper
def str_case_conversion(input_string,conversion = "lower"):
    is_empty,input_string = is_str_empty(input_string)
    if conversion == "lower":
        return input_string.upper()
    else:
        return input_string.lower()

#Mask the given string
def str_mask(input_string,replace_val="X",no_of_position_to_show = 4,mask_on = "left"):
    is_empty,input_string = is_str_empty(input_string)
    length  = len(input_string)
    if length == 0 or no_of_position_to_show >= length:
        return input_string
    replace_val = replace_val * (length - no_of_position_to_show)
    if mask_on == "left":
        return  replace_val + input_string[length - no_of_position_to_show:length]
    else:
        return  input_string[:no_of_position_to_show] + replace_val

#type conversion from one to another
def type_conversion(input,conversion="string"):

    input_string = is_str_empty(input)
    if conversion == "string":
        return input_string
    elif conversion == "int" and input_string.replace(".","").isdigit():
        return int(input_string)
    elif conversion == "float" and input_string.replace(".","").isdigit():
        f = float(input_string)
        return ":.2f".format(f)
    else:
        raise(f"Not able to do conversion {input_string}")





     

    
       
    