import json
import pandas as pd
import os
import csv
import datetime

SOURCE_1 = "personal_entries.json"
SOURCE_2 = "billing_entries.json"
SOURCE_3 = "sales_entries.csv"
OUTPUT   = "merged_data.xlsx"

def _convert_to_age(birth_year):
    '''
    A helper function to calculate age based on birth year
    '''
    now = datetime.datetime.now()
    return now.year - birth_year
    
def load_src1(json_path):
    '''
    Process the personal entries data source
    '''
    entries = json.load(open(json_path, 'r'))
    entry_list = list()
    for entry in entries:
        row = list()
        row.append(entry["PID"])
        row.append(entry["name"])
        row.append(entry["gender"])
        row.append(entry["last_contacted"])
        # Convert birth year to current age
        row.append(_convert_to_age(entry["birth_year"]))
        entry_list.append(row)
    
    return pd.DataFrame(entry_list, columns=["PID","name","gender","last_contacted","age"])
    
def load_src2(json_path):
    '''
    Process the billing entries data source
    '''
    entries = json.load(open(json_path, 'r'))
    entry_list = list()
    for entry in entries:
        row = list()
        row.append(str(entry["PID"]))
        row.append(entry["last_updated"])
        row.append(str(entry["address_info"]["ZIP"]))
        entry_list.append(row)
    
    return pd.DataFrame(entry_list, columns=["PID","last_updated","ZIP"])
    
def load_src3(csv_path):
    '''
    Process the offer entries data source
    '''
    entries = csv.reader(open(csv_path, 'r'), delimiter=';', quotechar='"')
    entry_list = list()
    for entry in entries:
        row = list()
        row.append(entry[0])
        row.append(entry[1])
        row.append(entry[2])
        entry_list.append(row)
    
    return pd.DataFrame(entry_list, columns=["PID","offer_date","offer_text"])

if __name__ == "__main__":
    df1 = load_src1(os.path.join("source", SOURCE_1))
    df2 = load_src2(os.path.join("source", SOURCE_2))
    df3 = load_src3(os.path.join("source", SOURCE_3))
    result = df1.merge(df2, on='PID').merge(df3, on='PID')
    # Replace empty values with explicit NaN
    for column in result:
        result[column].mask(result[column].astype('str') == "", "NaN", inplace=True)
        result[column].mask(result[column].astype('str') == "-1", "NaN", inplace=True)
    result.to_excel(OUTPUT)    

