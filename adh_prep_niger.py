# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 08:28:32 2022

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
    # find a word that is common to template and dataset
    values = ['Index',
              'Food ',
              'Tobacco',
              'Clothing',
              'Communication',
              'Education',
              'Housing',
              'Household',
              'Health',
              'Miscellaneous',
              'Culture',
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
    return df



def execute_old(data_path, country):
    #codes = pd.read_csv('./data/codeList.csv')
    # get template
    if '_' in country:
        c = country.split('_')
        c = [i.capitalize() for i in c]
        country2 = ' '.join(c)
    else:
        country2 = country.capitalize()

    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(1), lattice=True)
    df = tables[4]
    month = [val for key, val in months.items() if key.lower() in data_path.lower()][0]
    year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
    year = int(year)
    last = get_last_date_of_month(year, month)
    '''
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    '''
    '''
    # clean data
    df = df.drop([0,1])
    df = df.loc[:,['Unnamed: 1','Unnamed: 14']]
    df.columns = ['Indicator.Name',last]

    # translation
    try:
        df = translate_from_template(df,country)
    except:        
        df = translate_divisions(df)
        # save this in csv folder
        csv_folder = './data/%s/csv/'% country
        # create csv_folder folder
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)
        df_translated = df.drop(columns=[last])
        df_translated.to_csv(csv_folder+'translation_template.csv',index=False)
'''
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

def execute(data_path, country):
    if '_' in country:
        c = country.split('_')
        c = [i.capitalize() for i in c]
        country2 = ' '.join(c)
    else:
        country2 = country.capitalize()

    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(1), lattice=True)
    df = tables[4]
    month = [val for key, val in months.items() if key.lower() in data_path.lower()][0]
    year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
    year = int(year)
    last = get_last_date_of_month(year, month)
    
    # clean data
    df = df.loc[:,['Unnamed: 1','Unnamed: 14']]
    df.columns = ['Indicator.Name',last]

    # remove everything above first entry
    sub = 'INDICE' 
    x = df['Indicator.Name'].str.find(sub) # some have a space, so let's find the substring 
    row_drop = range(x[x==0].index.values[0])
    rows_drop = []
    for i in row_drop:
        rows_drop.append(i)
    
    df = df.drop(rows_drop)
    
    # translation
    try:
        df = translate_from_template(df,country)
    except:        
        df = translate_divisions(df)
        # save this in csv folder
        csv_folder = './data/%s/csv/'% country
        # create csv_folder folder
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)
        df_translated = df.drop(columns=[last])
        df_translated.to_csv(csv_folder+'translation_template.csv',index=False)

    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}_raw.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
    
    # map all items
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    df_1 = mapp_values(df,df_template)
    df_1.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
    
#%% check if there are new files
country = 'niger'
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