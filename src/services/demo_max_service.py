
import Config
import pandas as pd
import utils.max_mapping as mapping 
from utils.max_rules import rules
import datetime
import pytz
import os
import shutil

class DemoMax:

    def __init__(self):
        print("Initializing the class")
        self.comp_val = Config.properties.comp_val
        self.rules = rules()
    
    def proprocess_transaction(self,each_tran):
        if each_tran['msg_type'] not in ('Pacs_008','Pacs_009'):
            if "_" in str(each_tran['msg_type']):
                value_to_replace = str(each_tran['msg_type']).split("_")[1]
                if len(value_to_replace) < 2:
                    value_to_replace = "0"+value_to_replace
                each_tran['msg_type'] = value_to_replace
        if "msg_sub_type" in each_tran and len(str(each_tran['msg_sub_type'])) < 2:
            each_tran['msg_sub_type'] = "0" + str(each_tran['msg_sub_type'])
        if "instruction_id" in each_tran:
            each_tran["instruction_id"] = str(each_tran["instruction_id"]).replace("-"," ")
        if "settlement_amount" in each_tran:
            amount = str(each_tran["settlement_amount"])
            if "." not in amount:
                amount = str(int(amount) * 100)
            if "." in amount:
                amount = str(int(round(float(amount) * 100)))
            if len(amount) < 12:
                amount = "0" * (12 - len(amount)) + amount
            each_tran["settlement_amount"] = amount
        if "settlement_date" in each_tran:
            print(each_tran["settlement_date"])
            date = datetime.datetime.strptime(each_tran["settlement_date"],"%Y-%m-%d")
            each_tran["settlement_date"] = datetime.datetime.strftime(date,"%Y%m%d")

        return each_tran

    def get_val_1510(self,each_tran,temp_line,rep_val):
        if each_tran['msg_type'] not in ('Pacs_008','Pacs_009'):
            temp_line = temp_line.replace("{"+rep_val+"}",str(each_tran['msg_type'])+str(each_tran['msg_sub_type']))
        elif each_tran['msg_type'] in ('Pacs_008','Pacs_009') and each_tran['msg_type'] == 'CTR':
            temp_line = temp_line.replace("{"+rep_val+"}","1000")
        elif each_tran['msg_type'] in ('Pacs_008','Pacs_009') and each_tran['msg_type'] == 'BTR':
            temp_line = temp_line.replace("{"+rep_val+"}","1600")
        
        return temp_line

    def get_val(self,each_tran,temp_line,rep_val):
        if rep_val in each_tran:
            return temp_line.replace("{"+rep_val+"}",each_tran[rep_val])
        else:
            return temp_line.replace("{"+rep_val+"}","")
    
    def replace(self,temp_line,rep_key,rep_val):
        return temp_line.replace("{"+rep_key+"}",str(rep_val))
    
    def convert_trans_max_line(self,each_tran,index):
        try:
            line = ""
            each_tran = self.proprocess_transaction(each_tran)
            print("Started line converion")
            for each_key in mapping.mappingJson:
                temp_line = mapping.mappingJson[each_key]['line_val']
                replace_values = mapping.mappingJson[each_key]['line_replace'].split(',')
                skip_line = False
                if "rule" in  mapping.mappingJson[each_key]:
                    rule =  mapping.mappingJson[each_key]['rule']
                    match rule:
                        case "rule_10":
                            skip_line,each_tran = self.rules.rule_10(each_tran)
                        case "rule_3":
                            skip_line,each_tran = self.rules.rule_3(each_tran)
                        case "rule_2":
                            skip_line,each_tran = self.rules.rule_2(each_tran)
                        case "rule_4":
                            skip_line,each_tran = self.rules.rule_4(each_tran)
                        case "rule_5":
                            skip_line,each_tran = self.rules.rule_5(each_tran)
                        case "rule_6":
                            skip_line,each_tran = self.rules.rule_6(each_tran)
                        case "rule_7":
                            skip_line,each_tran = self.rules.rule_7(each_tran)
                        case "rule_8":
                            skip_line,each_tran = self.rules.rule_8(each_tran)
                        case "rule_9":
                            skip_line,each_tran = self.rules.rule_9(each_tran)
                        case "rule_11":
                            skip_line,each_tran = self.rules.rule_11(each_tran)
                        case "rule_12":
                            skip_line,each_tran = self.rules.rule_12(each_tran)
                        case "rule_13":
                            skip_line,each_tran = self.rules.rule_13(each_tran)
                        case _:
                            raise("Invalid rule mapping for the transaction.")
                for rep_val in replace_values:
                    if rep_val.startswith("config"):
                        temp_line = self.replace(temp_line,rep_val,Config.properties.__dict__[rep_val])
                    elif rep_val.startswith("line"):
                        match each_key:
                            case "1510":
                                temp_line = self.get_val_1510(each_tran,temp_line,rep_val)
                            case _:
                                temp_line = self.get_val(each_tran,temp_line,rep_val)
                    else:
                        if rep_val in each_tran:
                            temp_line = self.replace(temp_line,rep_val,each_tran[rep_val])
                        else:
                            temp_line = self.replace(temp_line,rep_val,"")

                if skip_line == False:
                    line = line + temp_line     
            print("Ended line converion")
            return True,line
        except Exception as exp:
            print("Exception while processing")
            return False,"line : " + str(index) + " " + str(exp)

    def get_max_trans_from_input(self,input_data_frame):
        print("Started doing convertion for all transactions")
        count = 0
        file_number = 1
        date_time_utc = datetime.datetime.now(tz=pytz.utc)
        str_date = date_time_utc.strftime("%m%d%Y%H%M%S%f")
        fileName = "MAX-" + str_date + "." +str(file_number)
        fileNameError = "ERROR-MAX-" + str_date + "." +str(file_number)
        file_write = open(os.path.join(Config.properties.max_success_location,fileName),"a+")
        input_data_frame.columns = [x.lower().replace(" ","_") for x in input_data_frame.columns]
        input_data_frame = input_data_frame.fillna("")
        input_data_frame = input_data_frame.astype(str)
        for index, each_trans in input_data_frame.iterrows():
            print(f"started parsing the line number - {index}")
            isSuccess,trans_line = self.convert_trans_max_line(each_trans,index)
            if isSuccess:
               file_write.write(trans_line+"\r\n") 
            else:
               file_write_error = open(os.path.join(Config.properties.max_error_location,fileNameError),"a+")
               file_write_error.write(trans_line+"\r\n") 
               file_write_error.close()
            count = count + 1
            if count == 100:
                file_write.close()
                file_number = file_number + 1
                fileName = "MAX-" + str_date + "." +str(file_number)
                file_write = open(os.path.join(Config.properties.max_success_location,fileName),"a+")
                count = 0

        if count > 0:
            file_write.close()

    
    def procees_conversion(self):
        #max_drop_location
        inputFile_dir = Config.properties.max_drop_location
        for inputFile in os.listdir(inputFile_dir):
            try:
                inputFile = os.path.join(Config.properties.max_drop_location,inputFile)
                print(f"Max File conversion for file - {inputFile}")
                if inputFile.endswith(".csv"):
                    df_input_tran = pd.read_csv(inputFile,delimiter=Config.properties.csv_delimiter) 
                elif  inputFile.endswith(".xlsx"):
                    df_input_tran = pd.read_excel(inputFile)
                elif inputFile.endswith(".txt"):
                    df_input_tran = pd.read_csv(inputFile,delimiter=Config.properties.csv_delimiter) 
                else:
                    raise("invalid file format")
                total_tra_count = df_input_tran.shape[0]
                if total_tra_count <= 0:
                    raise("Please provide the data as we are seeing empty file.")

                print("Valid file came with data and started doing the conversion.")
                self.get_max_trans_from_input(df_input_tran)
                destFile = os.path.join(Config.properties.max_archive_location,os.path.basename(inputFile))
                shutil.move(inputFile,destFile)
            except Exception as exp:
                destFile = os.path.join(Config.properties.max_error_location,os.path.basename(inputFile))
                shutil.move(inputFile,destFile)
