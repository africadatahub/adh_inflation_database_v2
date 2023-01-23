# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 09:45:13 2022

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
              'Food and non-',
              'Tobacco',
              'Clothing',
              'Communication',
              'Education',
              'Housing',
              'Household',
              'Health',
              'Miscellaneous',
              'Recreation',
              'Restaurants',
              'Transport',
              'Insurance']
        
    for i in range(len(values)):
        val = template[template['Indicator.Name'].str.contains(values[i],case=False)==True]
        try:
            df['Indicator.Name'][df['Indicator.Name'].str.contains(values[i],case=False)==True] = val['Indicator.Name'].values
        except:
            print('ERROR with: {}'.format(values[i]))    
    df = pd.merge(template,df,how='left',on = 'Indicator.Name')
    df = df.round(2)
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

    month = int(data_path[32:34])
    year = int(data_path[34:38])
    last = get_last_date_of_month(year, month)
    col = get_first_date_of_month(year, month)
    #codes = pd.read_csv('./data/codeList.csv')
    #df = pd.read_excel(data_path,skiprows= 20, nrows= 14)
    df = pd.read_excel(data_path,sheet_name='{}_REBASED SERIES'.format(str(year)),nrows=17)
    df = df.iloc[:,[1,month+3]]
    df.columns = ['Indicator.Name',year]
    #df.columns=['Indicator.Name',last]
    df_prev = pd.read_excel(data_path,sheet_name='{}_REBASED SERIES'.format(str(year-1)),nrows=17)
    df_prev = df_prev.iloc[:,[1,month+3]]
    df_prev.columns = ['Indicator.Name',year-1]
    df = pd.merge(df_prev,df,how='left',on = 'Indicator.Name')
    
    df = df.drop([0,16])
    
    df['change'] = df[year]-df[year-1]
    df['perc'] = (df.change/df[year-1])*100
    df = df.loc[:,['Indicator.Name','perc']]
    df.columns = ['Indicator.Name',last]
    df[last] = df[last].astype(float)
    
    '''
    # get template
    data_path= './outputs/ckan/'
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    '''
    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
    
    
#%% check if there are new files
country = 'tanzania'
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
        data_path = files_list[i].split('.pdf')[0]
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
            data_path = file.files.to_list()[i].split('.pdf')[0]
            print(data_path)
            try:
                execute(data_path, country)
                f.write(file.files.to_list()[i])
                f.write('\n')
            except:
                print('failed %s'% data_path)
        f.close()
    else:
        print('No new %s country data'% country)
        
#%%

def template(country):
    #codes = pd.read_csv('./data/codeList.csv')
    # get template
    if '_' in country:
        c = country.split('_')
        c = [i.capitalize() for i in c]
        country2 = ' '.join(c)
    else:
        country2 = country.capitalize()
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    #df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    
    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    df_template.to_csv('{}{}_template.csv'.format(csv_folder,country),index=False)

#template(country)