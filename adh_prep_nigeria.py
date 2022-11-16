# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 10:41:45 2022

@author: heiko
"""

import pandas as pd
from datetime import date, datetime, timedelta
#import numpy as np
import glob, shutil
import re, os

# functions and definitions

def get_last_date_of_month(year, month):
    """Return the last date of the month.
    
    Args:
        year (int): Year, i.e. 2022
        month (int): Month, i.e. 1 for January

    Returns:
        date (datetime): Last date of the current month
    """
    
    if month == 12:
        last_date = datetime(year, month, 31)
    else:
        last_date = datetime(year, month + 1, 1) + timedelta(days=-1)
    
    return last_date.strftime("%Y-%m-%d")

def get_first_date_of_month(year, month):

    first_date = datetime(year, month, 1)
    
    return first_date.strftime("%Y-%m-%d")

months = dict({'Jan':1,
               'Feb':2,
               'Mar':3,
               'Apr':4,
               'May':5,
               'Jun':6,
               'Jul':7,
               'Aug':8,
               'Sep':9,
               'Oct':10,
               'Nov':11,
               'Dec':12
               })

def mapp_values(df,template):
    template = template.loc[:,['Indicator.Name','Indicator.Code']]
    values = ['All',
              'Food',
              'Tobacco',
              'Clothing',
              'Communication',
              'Education',
              'Housing',
              'Household',
              'Health',
              'Miscellaneous',
              'Recreation',
              'Restaurant',
              'Transport']
        
    for i in range(len(values)):
        val = template[template['Indicator.Name'].str.contains(values[i],case=False)==True]
        try:
            df['Indicator.Name'][df['Indicator.Name'].str.contains(values[i],case=False)==True] = val['Indicator.Name'].values
        except:
            print('ERROR with: {}'.format(values[i]))    
    df = pd.merge(template,df,how='left',on = 'Indicator.Name')
    df = df.round(2)
    return df

def prep(df):
    df = df.drop(columns=['Unnamed: 0','Month-on (%)', 'Year-on (%)', '12-month average (%)'])
    df = df.set_index('Unnamed: 1')
    df = df.transpose()
    
    return df


def execute(data_path, country):
    #codes = pd.read_csv('./data/codeList.csv')
    # get template
    if '_' in country:
        c = country.split('_')
        c = [i.capitalize() for i in c]
        country2 = ' '.join(c)
    else:
        country2 = country.capitalize()
    
    #month = [val for key, val in months.items() if key.lower() in data_path][0]
    month = 10
    year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
    year = int(year)
    last = get_last_date_of_month(year, month)
    
    df = pd.read_excel(data_path,sheet_name='Table2',header=1)
    df = df.drop(0)
    df = df.drop(columns=['Unnamed: 22'])
    
    df_2021 = df.iloc[312:324,:]
    df_2022 = df.iloc[324:,:]
    df_2021 = prep(df_2021)
    df_2022 = prep(df_2022)
    df_2021 = df_2021.loc[:,df_2022.columns]
    df = ((df_2022-df_2021)/df_2021)*100
    df = df.reset_index()
    df = df.rename(columns={'index':'Indicator.Name'})
    df = df.drop([1,2,3,4])
    
    # get template
   
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    df_template = df_template[df_template['Indicator.Name']!='Insurance and Financial Services']
    # map all items
    df_1 = mapp_values(df,df_template)
    
    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df_1.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1].split('.')[0]),index=False)


#%% check if there are new files
country = 'nigeria'
base_data_path ='./data/%s/raw/'% country
files_list = glob.glob('%s*.xlsx'% base_data_path)
files_list_2 = glob.glob('%s*.xls'% base_data_path)
files_list = files_list + files_list_2
for i in range(len(files_list)):
    files_list[i] = files_list[i].replace("\\","/") 
#%%  
    
#check for data log  
data_log = glob.glob('%sdata_log.txt'% base_data_path)
if len(data_log)==0:
    f = open('%sdata_log.txt'% base_data_path,'w')
    for i in range(len(files_list)):
        data_path = files_list[i]#.split('.xls')[0]
        execute(data_path, country)
        f.write(files_list[i])
        f.write('\n')
    f.close()
    
else:    
    logs = pd.read_csv('%sdata_log.txt'% base_data_path,header=None)
    logs.columns=['done']
    logs = logs.done.to_list()
    files = pd.DataFrame()
    files['files'] = files_list
    file = files[~files.files.isin(logs)]

    if len(file) != 0:
        print('Preparing %s data...'% country)
        
        f = open('%sdata_log.txt'% base_data_path,'a')
        for i in range(len(file)):
            data_path = file.files.to_list()[i]#.split('.xls')[0]
            print(data_path)
            execute(data_path, country)
            f.write(files_list[i])
            f.write('\n')
        f.close()
    else:
        print('No new %s country data'% country)
        
#%%


#%%




