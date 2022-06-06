#!/usr/bin/env python3
#Author: Jingyuan
#Date: 2022-06-05
#Data storage: GCP bucket
#Computational platform: GCP (Google Collab)

import pandas as pd
import numpy as np
import os
from pathlib import Path

## Read in data
file_path = Path(os.getcwd()+'/Example_Data.xlsx')
file_extension = file_path.suffix.lower()[1:]
raw_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
Example_Answer = raw_data['Example_Answer']
Example_DB = raw_data['Example_DB']
Example_Data = raw_data['Example_Data']
del raw_data

## First-step preprocessing
new_header = Example_Data.iloc[0].values
Example_Data = Example_Data.iloc[1:,:]
Example_Data.columns = new_header

## Column names
company_info = list(Example_Data.columns[:6])
matric_info = list(Example_Data.columns[6:11])

## Clean and preprocess data
def clean_preprocess():
    
    print("Clean Example data:...")
    # Set matrics related columns to integer
    Example_Data[matric_info] = Example_Data[matric_info].astype("Int64")
    # Trim space for string columns
    Example_Data[company_info] = Example_Data[company_info].astype("str").apply(lambda x: x.str.strip())
    # Handle Company ID vs Company Name
    ## Find records that needes cleaning
    company_id_name_count = (Example_Data
                             .groupby(['Company ID','Company Name'])
                             .size()
                             .reset_index()[['Company ID','Company Name']]
                             .sort_values(by='Company ID')
                             .groupby('Company ID')
                             .size())
    ID_flg = company_id_name_count[np.where(company_id_name_count>1)[0]].reset_index()['Company ID'].values
    company_name_id_count = (Example_Data
                             .groupby(['Company ID','Company Name'])
                             .size()
                             .reset_index()[['Company ID','Company Name']]
                             .sort_values(by='Company Name')
                             .groupby('Company Name')
                             .size())
    name_flg = company_name_id_count[np.where(company_name_id_count>1)[0]].reset_index()['Company Name'].values
    
    company_id_name_lookup = (Example_Data
                              .groupby(['Company ID','Company Name','Industry'])
                              .size()
                              .reset_index()[['Company ID','Company Name','Industry']])
    company_id_name_lookup_check = company_id_name_lookup[~(company_id_name_lookup['Company ID'].isin(ID_flg) | company_id_name_lookup['Company Name'].isin(name_flg))]
    company_id_name_lookup_flg = company_id_name_lookup[company_id_name_lookup['Company ID'].isin(ID_flg) | company_id_name_lookup['Company Name'].isin(name_flg)]
    ## Introduce database data for reference
    db_company_id_name_lookup = Example_DB.groupby(['Company ID','Company Name','Industry']).size().reset_index()[['Company ID','Company Name','Industry']].astype(str)
    ## Issue 1: Multiple names to 1D
    ID_problem = company_id_name_lookup_flg[company_id_name_lookup_flg['Company ID'].isin(ID_flg)]
    ### New record, use the name that matches the database format
    ID_problem_corrected = ID_problem.sort_values(by='Company Name').groupby('Company ID').first().reset_index()
    ## Fix Issue 1
    cleaned_data = pd.merge(Example_Data,ID_problem_corrected,on=['Company ID','Industry'],how="left") 
    cleaned_data['Company Name'] = np.where(cleaned_data['Company ID'].isin(ID_flg), cleaned_data['Company Name_y'],cleaned_data['Company Name_x'])
    cleaned_data = cleaned_data.drop(['Company Name_x','Company Name_y'],axis=1)
    
    # Fiscal Year
    ## In year '2Q16', Q should be replaced by 0
    cleaned_data['Fiscal Year'] = np.where(cleaned_data['Fiscal Year']=='2Q16','2016',cleaned_data['Fiscal Year']).astype(int)
    ## Missing 0 in year '213'
    cleaned_data['Fiscal Year'] = np.where(cleaned_data['Fiscal Year']==213,
                                           2013,
                                           cleaned_data['Fiscal Year'])
    
    # Check SIC Code
    SICCode = cleaned_data['SIC Code']
    SIC_flg = SICCode[(SICCode.str.len()!=4)|(~SICCode.str.isdigit())]
    db_lookup_sic = (Example_DB
                     .groupby(['Company ID','Company Name','Industry','SIC Code'])
                     .size()
                     .reset_index()[['Company ID','Company Name','Industry','SIC Code']]
                     .astype("str"))
    ## Fix SIC Code
    sic_fix = pd.merge(cleaned_data[cleaned_data['SIC Code'].isin(SIC_flg)],
                       db_lookup_sic,
                       on=['Company ID','Company Name','Industry'],
                       how="left")
    sic_fix['SIC Code'] = sic_fix['SIC Code_y']
    sic_fix = sic_fix.drop(['SIC Code_x','SIC Code_y'],axis=1)
    ## B Company SIC Code
    BCompanySIC = (cleaned_data[cleaned_data['Company Name']=='B Company']
               .groupby('SIC Code')
               .size().reset_index()
               .sort_values(by=0,ascending=False)
               .iloc[0,0])
    sic_fix.loc[sic_fix['Company Name'] == 'B Company', 'SIC Code'] = BCompanySIC
    ## Apply all fix
    cleaned_fix = pd.merge(cleaned_data,
                           sic_fix[['Company ID','Company Name','Industry','SIC Code']],
                           on=['Company ID','Company Name','Industry'],
                           how="left")
    cleaned_fix['SIC Code'] = np.where(cleaned_fix['SIC Code_y'].isnull(),
                                       cleaned_fix['SIC Code_x'],
                                       cleaned_fix['SIC Code_y'])
    cleaned_data = cleaned_fix.drop(['SIC Code_x','SIC Code_y'],axis=1).copy()
    del cleaned_fix
    
    # Check Currency
    currency_array = cleaned_data['Trading Currency']
    currency_array = np.where(currency_array=='3$','USD',currency_array)
          
    cleaned_data['Trading Currency'] = currency_array
    
    # Checkpoint 1: save cleaned Example Data in csv
    print("Cleaning done.")
    print("Save cleaned data:...")
    cleaned_data.to_csv('example_data_clean.csv',index=False)
    print("Checkpoint 1: Done.")
    
    # Formatting Data to Match with Database
    print("Formatting data:...")
    cleaned_data_format = cleaned_data.set_index(['Company ID', 'Company Name', 'Fiscal Year', 'Industry', 'SIC Code','Trading Currency'])
    cleaned_data_format = cleaned_data_format.stack(dropna=False).reset_index()
    cleaned_data_format.columns = ['Company ID', 'Company Name', 'Fiscal Year', 'Industry', 'SIC Code','Trading Currency', 'Metric Name', 'Value']
    cleaned_data_format = cleaned_data_format.sort_values(by = ['Metric Name','Company Name','Company ID','Industry','Fiscal Year'],ascending=[False,True,True,True,True])
    
    # Checkpoint 2: Formatted clean data
    print("Formatting done.")
    print("Save formatted data:...")
    cleaned_data_format.to_csv('example_data_clean_formatted.csv',index=False)
    print("Checkpoint 2: Done.")
    
    # Compare data in and in database
    ## Align data types
    print("Executing Comparison:...")
    Example_DB[['Company ID','Company Name','Industry','SIC Code','Trading Currency','Metric Name']] = Example_DB[['Company ID','Company Name','Industry','SIC Code','Trading Currency','Metric Name']].astype(str)
    Example_DB[['Fiscal Year','Value']] = Example_DB[['Fiscal Year','Value']].astype('Int64')
    col_names = list(Example_DB.columns)
    ## Tag each source
    Example_DB['data_source'] = 'DB'
    cleaned_data_format['data_source'] = 'data'
    ## Execute comparison
    compare = pd.merge(cleaned_data_format,Example_DB,how="outer",on=col_names[:-1], suffixes=('_data', '_db'))
    ## Establish dataframe to save results
    Example_Compare = pd.DataFrame()
    ## 1. Values not equal
    matched_data = compare[(~compare.data_source_data.isnull())&(~compare.data_source_db.isnull())]
    value_not_equal = matched_data[matched_data.Value_data!=matched_data.Value_db][col_names[:-1]+['Value_data','Value_db']]
    one_is_null = matched_data[(matched_data.Value_data.isnull()&~matched_data.Value_db.isnull())|(~matched_data.Value_data.isnull()&matched_data.Value_db.isnull())][col_names[:-1]+['Value_data','Value_db']]
    UnEqual = pd.concat([value_not_equal,one_is_null])
    UnEqual['ERROR Type'] = 'UnEqual'
    Example_Compare = pd.concat([Example_Compare,UnEqual])
    ## 2. Data in file but not in DB
    in_file_not_db = compare[(~compare.data_source_data.isnull())&(compare.data_source_db.isnull())][col_names[:-1]+['Value_data','Value_db']]
    in_file_not_db['ERROR Type'] = 'Not_in_DB'
    Example_Compare = pd.concat([Example_Compare,in_file_not_db])
    ## 3. Data in DB but not in file
    in_db_not_file = compare[(compare.data_source_data.isnull())&(~compare.data_source_db.isnull())][col_names[:-1]+['Value_data','Value_db']]
    in_db_not_file['ERROR Type'] = 'Not_in_File'
    Example_Compare = pd.concat([Example_Compare,in_db_not_file])
    print("Print out problematic data:")
    print(Example_Compare)
    # Checkpoint 3: Save the records with errors
    print("Comparison done.")
    print("Save Comparison results:...")
    Example_Compare.to_csv('example_data_vs_db.csv',index=False)
    print("Checkpoint 3: Done.")
    print("Program execution complete.")
    
    return None

execution = clean_preprocess()
    
    
    
    
    
    
    
    
    
    
    
    
    