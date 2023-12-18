
import Config
import pandas as pd
import utils.mt103_mapping as mapping 
import datetime
import pytz
import os
import shutil
from data_enrichment_service import DataEnrichment

class Demo103:

    def __init__(self):
        print("Initializing the class")
        self.comp_val = Config.properties.comp_val
        self.dataEnrichment = DataEnrichment()

    def replace(self,temp_line,rep_key,rep_val):
        return temp_line.replace("<"+rep_key+">",str(rep_val))
    
    def convert_trans_mt103_line(self,each_tran,index):
        try:
            line = ""
            print("Started line converion")
            for each_key in mapping.mappingJson:
                temp_line = mapping.mappingJson[each_key]['line_val']
                replace_values = mapping.mappingJson[each_key]['line_replace'].split(',')
                skip_line = False
                for rep_val in replace_values:
                    if rep_val.startswith("config") or rep_val == "replace_common":
                        temp_line = self.replace(temp_line,rep_val,Config.properties.__dict__[rep_val])
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

    def get_mt103_trans_from_input(self,input_data_frame):
        print("Started doing convertion for all transactions")
        count = 0
        file_number = 1
        date_time_utc = datetime.datetime.now(tz=pytz.utc)
        str_date = date_time_utc.strftime("%m%d%Y%H%M%S%f")
        fileNameError = "ERROR-MT103-" + str_date + "." +str(file_number)        
        input_data_frame.columns = [x.lower().replace(" ","_") for x in input_data_frame.columns]
        input_data_frame = input_data_frame.fillna("")
        input_data_frame = input_data_frame.astype(str)
        for index, each_trans in input_data_frame.iterrows():
            print(f"started parsing the line number - {index}")
            isSuccess,trans_line = self.convert_trans_mt103_line(each_trans,index)
            if isSuccess:
               fileName = "MT103-" + str_date + "." +str(file_number)+".bak"
               file_write = open(os.path.join(Config.properties.mt103_success_location,fileName),"a+")
               file_write.write(trans_line+"\r\n") 
            else:
               file_write_error = open(os.path.join(Config.properties.mt103_error_location,fileNameError),"a+")
               file_write_error.write(trans_line+"\r\n") 
               file_write_error.close()
            count = count + 1
            if count == 1:
                file_write.close()
                file_number = file_number + 1

        if count > 0:
            file_write.close()

    
    
    def procees_conversion(self,input_json):
        inputFile = "D:\\project\\ajay\\files\\mt103filenew.xlsx"
        inputFile_dir = Config.properties.mt103_drop_location
        for inputFile in os.listdir(inputFile_dir):
            try:
                inputFile = os.path.join(Config.properties.mt103_drop_location,inputFile)
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
                df_input_tran = self.dataEnrichment.perform_data_enrich_ment(df_input_tran,input_json)
                print("Valid file came with data and started doing the conversion.")
                self.get_mt103_trans_from_input(df_input_tran)
                destFile = os.path.join(Config.properties.mt103_archive_location,os.path.basename(inputFile))
                shutil.move(inputFile,destFile)
            except Exception as exp:
                destFile = os.path.join(Config.properties.mt103_error_location,os.path.basename(inputFile))
                shutil.move(inputFile,destFile)
            
