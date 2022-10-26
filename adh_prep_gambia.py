# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 13:37:30 2022

@author: heiko
"""
import tabula
import pandas as pd
from datetime import datetime, timedelta
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

def execute(data_path, country):
    #codes = pd.read_csv('./data/codeList.csv')
    # get template
    if '_' in country:
        c = country.split('_')
        c = [i.capitalize() for i in c]
        country2 = ' '.join(c)
    else:
        country2 = country.capitalize()

    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(2), lattice=True)
    country2 = 'Kenya'
    month = [val for key, val in months.items() if key.lower() in data_path.lower()][0]
    year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
    year = int(year)
    last = get_last_date_of_month(year, month)
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    
    if len(tables)==1: 
        df = tables[0]
        df.columns = df.iloc[1]
        df = df[2:]
        df = df.loc[:,['COICOP','ALL  ITEMS(JANUARY 2020=100)','12 Months']]
        keep = ['0','01','02','03','04','05','06','07','08','09','10','11','12']
        df.columns = ['COICOP','Indicator.Name',last]
        df = df[df.COICOP.isin(keep)]
        df = df.drop(columns=['COICOP'])
    else:
        df = tables[1]
        df.columns = df.iloc[1]
        df = df.loc[:,['ALL  ITEMS(JANUARY 2020=100)','12 Months']]
        df = df.drop([0,1,4,5,6,7,8,9,10,11,12,13,14,15,17,18,20,21,22,23,24,26,27,28,30,31,32,33,38,39,43,44,45,46,47,48])
        df.columns = ['Indicator.Name',last]
    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
    
    
    '''
    # map all items
    df_1 = mapp_values(df,df_template)
    # create outputs folder folder
    if not os.path.exists('./outputs/%s'% country):
        os.makedirs('./outputs/%s'% country)
    df_1.to_csv('./outputs/{}/{}_{}.csv'.format(country,country,last),index=False)
    '''
    
    
#%% check if there are new files
country = 'gambia'
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
            execute(data_path, country)
            f.write(files_list[i])
            f.write('\n')
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
