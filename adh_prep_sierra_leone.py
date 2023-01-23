# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 11:15:22 2022

@author: heiko
"""

import pandas as pd
from datetime import datetime, timedelta
import glob, shutil
import re, os, tabula
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
    # find a word that is common to template and dataset
    values = ['All',
              'Food ',
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

#%%

def get_template(df):
    # pattern: nans occur in sentences, so each sentence needs to be joined on either side of the nan
    # loop through indicator name to find nans
    template = []
    
    df['Indicator.Name'] = df['Indicator.Name'].fillna('NAN')
    
    indic = df['Indicator.Name'].to_list()
    
    for i in range(len(indic)):
        if indic[i]!='NAN':
            template.append(i)
        else:
            template.append('NAN')
    for k in range(len(template)):
        if template[k] == 'NAN':
            template[k] = template[k-1]
            template[k+1] = template[k-1]
    return template

def execute_1(tables,last):
    df = tables[0]
    df = df.T.reset_index().T
    df = df.reset_index(drop=True)
    df = df.iloc[:,[0,-1]]

    df.columns = ['Indicator.Name',last]
    # labels
    df_labels = df.iloc[:,[0]]
    template = [0,1,1,2,3,3,4,4,5,6,7,8,9,10,11,12]
    try:
        df['template'] = template
    except:
        print('trying dynamic templating')
        df = execute_2(tables,last)
        return df
    df = df.drop(columns=['Indicator.Name'])
    df = df.dropna()
    df_labels['template'] = template
    df_labels = df_labels.dropna()
    df_labels = df_labels.groupby(['template'])['Indicator.Name'].apply(' '.join).reset_index()

    df = pd.merge(df,df_labels,how='left',on='template')
    df = df.drop(columns='template')
    return df

def execute_2(tables,last):
    df = tables[0]
    df = df.T.reset_index().T
    df = df.reset_index(drop=True)
    df[0] = df[0].fillna('')
    df[1] = df[1].fillna('')
    df[0] = df[0].astype(str) +' ' + df[1].astype(str)
    df = df.iloc[:,[0,-1]]

    df.columns = ['Indicator.Name',last]
    # remove all rows above first entry
    sub = 'Food'
    x = df['Indicator.Name'].str.find(sub)
    row_drop = range(x[x==0].index.values[0])
    rows_drop = []
    for i in row_drop:
        rows_drop.append(i)
    
    df = df.drop(rows_drop)
    df = df.dropna()
    
    # fix some names
    df['Indicator.Name'][df['Indicator.Name'].str.contains('Beverages',case=False)==True] = "Food "
    df['Indicator.Name'][df['Indicator.Name'].str.contains('Narcotics',case=False)==True] = "Tobacco"
    df['Indicator.Name'][df['Indicator.Name'].str.contains('Clothing',case=False)==True] = "Clothing"
    df['Indicator.Name'][df['Indicator.Name'].str.contains('Fuels',case=False)==True] = "Housing"
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
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]

    month = [val for key, val in months.items() if key in data_path][0]
    year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
    year = int(year)
    last = get_last_date_of_month(year, month)

    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(6), stream=True)
    if len(tables) == 0:
        tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(7), stream=True)
        doctype = 1 
    else:
        doctype = 0
    if doctype == 1:
        print('doctype 1')
        df = execute_2(tables,last)
    else:
        print('doctype 0')
        df = execute_1(tables,last)

    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}_raw.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)

    # map values
    df_1 = mapp_values(df,df_template)
    df_1.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)

#%% check if there are new files
country = 'sierra_leone'
base_data_path ='./data/%s/raw/'% country
files_list = glob.glob('%s*.pdf'% base_data_path)
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



