import pandas as pd

worksheet_name = 'Goal2021_New_Final'
df = pd.read_excel(worksheet_name+".xlsx", sheet_name=None)  
for key in df.keys(): 
    df[key].to_csv('output/{}--{}.csv'.format(worksheet_name,key))