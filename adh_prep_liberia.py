# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 15:35:19 2022

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

#%% execute 

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


def execute(data_path, country):
    # Note - Liberia is somewhat unique, so we need to take a different approach
    # for one thing, the latest file as data for the entire year, so we only need to look at that
    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(15), stream=True)
    df_1 = tables[0]
    df_2 = tables[1]

    df = pd.merge(df_1,df_2,how='left',on=['Function','Weight (%)'])
    df = df.rename(columns={'Function':'Indicator.Name'})
    
    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}_raw.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
    
    df = df.drop(columns=['Weight (%)'])
    df_labels = df.iloc[:,[0]]
    template = get_template(df)
    df['template'] = template
    df = df.drop(columns=['Indicator.Name'])
    df = df.dropna()
    df_labels['template'] = template
    df_labels = df_labels.dropna()
    df_labels = df_labels.groupby(['template'])['Indicator.Name'].apply(' '.join).reset_index()

    df = pd.merge(df,df_labels,how='left',on='template')
    df = df.drop(columns='template')
    
    df['Indicator.Name'][df['Indicator.Name'].str.contains('General Rate',case=False)==True] = "All"
    
    # get country template
    country2 = country.capitalize()
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]
    
    # map all items
    df_1 = mapp_values(df,df_template)
    
    df_1.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)


def execute_old(data_path, country):
    #codes = pd.read_csv('./data/codeList.csv')
    # get template
    country2 = 'Liberia'
    file = './outputs/ckan/bk/template.csv'
    df_template = pd.read_csv(file)
    df_template = df_template[df_template['Country']==country2]
    df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]

    tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(33,34,35), stream=True)
    for k in range(len(tables)):
        df = tables[k]
        df = df.rename(columns={'Function':'divisions'})
        # remove special character
        df.columns = df.columns.str.replace(' ', '')
        df.divisions = df.divisions.replace(['General Rate of Inflation'],'All')
        # fix columns
        cols = [col for col in df.columns if col.strip().split('-')[0] in months.keys()]
        cols = ['divisions'] + cols
        df = df.loc[:,cols]
        # labels
        df_labels = df.iloc[:,[0]]
        template = [0,1,2,3,4,4,4,5,6,7,8,9,10,11,12]
        df['template'] = template
        df = df.drop(columns=['divisions'])
        df = df.dropna()
        df_labels['template'] = template
        df_labels = df_labels.dropna()
        df_labels = df_labels.groupby(['template'])['divisions'].apply(' '.join).reset_index()

        df = pd.merge(df,df_labels,how='left',on='template')
        df = df.drop(columns='template')

        df = df.loc[:,cols]
        # save this in csv folder
        csv_folder = './data/%s/csv/'% country
        # create csv_folder folder
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)
        df.to_csv('{}{}_{}.csv'.format(csv_folder,data_path.split('raw/')[1],k),index=False)
        '''
        for z in range(1,len(df.columns)):
            mon = df.columns[z]
            month = [val for key, val in months.items() if key in mon][0]
            year = int('20'+ mon.split('-')[1])
            df_mon = df.loc[:,['divisions',mon]]
            last = get_last_date_of_month(year, month)
            df_mon.columns = ['Indicator.Name',last]
            # map all items
            df_1 = mapp_values(df_mon,df_template)
            # create outputs folder folder
            if not os.path.exists('./outputs/%s'% country):
                os.makedirs('./outputs/%s'% country)
            df_1.to_csv('./outputs/{}/{}_{}.csv'.format(country,country,last),index=False)

        '''                     


#%% check if there are new files
country = 'liberia'
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
        execute = False

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