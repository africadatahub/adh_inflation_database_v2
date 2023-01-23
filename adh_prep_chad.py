# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 14:10:42 2022

@author: heiko
"""

import tabula
import pandas as pd
from datetime import datetime, timedelta
import glob, shutil
import re, os
from translate import Translator

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
               'Fev':2,
               'Mar':3,
               'Avr':4,
               'Mai':5,
               'Juin':6,
               'Juil':7,
               'Aout':8,
               'Sep':9,
               'Oct':10,
               'Nov':11,
               'Dec':12
               })

def mapp_values(df,template):
    template = template.loc[:,['Indicator.Name','Indicator.Code']]
    # note: unique to Chad
    values = ['Index',
              'Food',
              'Tobacco',
              'Clothing',
              'Communication',
              'Education',
              'Housing',
              'Household',
              'Health',
              'Miscellaneous',
              'Culture',
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

#%% 
def translate_from_template(df,country):
    csv_folder = './data/%s/csv/translation_template.csv'% country
    df_2 = pd.read_csv(csv_folder)
    for i in range(len(df)):
        df['Indicator.Name'][df['Indicator.Name'].str.contains(df_2.iloc[i,0][0:20],case=False)==True] = df_2.iloc[i,0]
    df = df.rename(columns={'Indicator.Name':'original_language'})
    df = pd.merge(df,df_2, how = 'left',on='original_language')
    return df
    

def translate_divisions(df):
    divs = df['Indicator.Name'].to_list()
    translator= Translator(from_lang="french",to_lang="english")
    divs2 = []
    for i in range(0,len(divs)):
        try:
            divs2.append(translator.translate(divs[i]))
        except:
            try:
                divs2.append(translator.translate(divs[i].split(' ')[2]))
            except:
                print('tanslation failed: {}'.format(divs[i]))
                divs2.append(divs[i])
    df = df.rename(columns={'Indicator.Name': 'original_language'})            
    df['Indicator.Name'] = divs2
    #translation = translator.translate("Guten Morgen")
    #divs2 = [translator.translate(x) for x in divs]
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
    '''    
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    '''
    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(2), stream=True)
    if len(tables)==2:
        # want table with more columns
        df_1 = tables[0]
        df_2 = tables[1]
        c1 = len(df_1.axes[1])
        c2 = len(df_2.axes[1])
        if c1 > c2:
            df = tables[0]
        else:
            df = tables[1]
            
        df = df.drop([0,1])
    else:
        df = tables[0]
        '''
        if data_path in ['./data/chad/raw/Bulletin-INPC-04-2022','./data/chad/raw/Bulletin-INPC-05-2022']:
            drop_rows = range(0,len(df)-29)
        else:
            drop_rows = range(0,len(df)-30)
        df = df.drop(drop_rows)
        '''
    '''
    if len(df) == 36:
        df = df.drop([0,1,2,3,4,5])
    else:    
        df = df.drop([0,1])
    ''' 

    df = df.iloc[:,[1,-1]]
    
    # remove everything above Produits
    df = df.T.reset_index().T
    row_drop = range(df[df[0].str.contains('Produits')].index.values[0])
    rows_drop = []
    for i in row_drop:
        rows_drop.append(i)
    
    df = df.drop(rows_drop)
    
    
    # remove rows with all nans
    df = df[~df.isnull().all(axis=1)]
    month = int(data_path[30:32])
    year = int(data_path[33:])
    last = get_last_date_of_month(year, month)
    df.columns = ['Indicator.Name',last]

    # labels
    df_labels = df.iloc[:,[0]]
    template = [0,0,0,1,2,3,4,5,6,7,7,7,8,8,8,9,9,9,10,10,10,11,12,13,14,15,16,17,18]
    df['template'] = template
    df = df.drop(columns=['Indicator.Name'])
    df = df.dropna()
    df_labels['template'] = template
    df_labels = df_labels.dropna()
    df_labels = df_labels.groupby(['template'])['Indicator.Name'].apply(' '.join).reset_index()

    df = pd.merge(df,df_labels,how='left',on='template')
    df = df.drop([1,2,3,4,5,6])
    df = df.drop(columns='template')
    df = df.loc[:,['Indicator.Name',last]]
   
    # translation
    try:
        df = translate_from_template(df,country)
    except:        
        df = translate_divisions(df)

    df['Indicator.Name'][df['Indicator.Name'].str.contains('Teaching',case=False)==True] = "Education"

    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
'''
    df = df.loc[:,['Indicator.Name',last]]
    # map all items
    df_1 = mapp_values(df,df_template)
    # create outputs folder folder
    if not os.path.exists('./outputs/%s'% country):
        os.makedirs('./outputs/%s'% country)
    df_1.to_csv('./outputs/{}/{}_{}.csv'.format(country,country,last),index=False)
'''


#%% check if there are new files
country = 'chad'
base_data_path ='./data/%s/raw/'% country
files_list = glob.glob('%s*.pdf'% base_data_path)
for i in range(len(files_list)):
    files_list[i] = files_list[i].replace("\\","/") 
#%%  
failed = []    
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
                f.write(files_list[i])
                f.write('\n')
            except:
                print('{} failed'.format(data_path))
                failed.append(data_path)
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



