import pandas as pd


inputFile = "D:\\project\\ajay\\files\\mt103filenew.xlsx"
df_input_tran = pd.read_excel(inputFile)
df_input_tran.to_json("D:\\project\\ajay\\files\\mt103filenew.json",orient="records")