import pandas as pd

worksheet_name = 'Goal2021_New_Final'
file_name = 'Goal2021_New_Final.xlsx'
df = pd.read_excel(file_name, sheet_name=None)  
for key in df.keys(): 
    df[key].to_csv('output/{}--{}.csv'.format(worksheet_name,key))