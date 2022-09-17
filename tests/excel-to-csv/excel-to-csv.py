import pandas as pd

file_name = 'Goal2021_New_Final'
object_key = 'Goal2021_New_Final.xlsx'
df = pd.read_excel(object_key, sheet_name=None)  
for key in df.keys(): 
    df[key].to_csv('output/{}--{}.csv'.format(file_name,key))