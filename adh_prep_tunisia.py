# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 13:30:05 2022

@author: heiko
"""

import pandas as pd
from datetime import datetime, timedelta
import glob, shutil
import re, os

def execute(data_path, country):
    #codes = pd.read_csv('./data/codeList.csv')
    # get template
    if '_' in country:
        c = country.split('_')
        c = [i.capitalize() for i in c]
        country2 = ' '.join(c)
    else:
        country2 = country.capitalize()
    
    df = pd.read_csv("{}.csv".format(data_path),sep = ';',skiprows = [0,1,2,3,4,5])
    # save this in csv folder
    csv_folder = './data/%s/csv/'% country
    # create csv_folder folder
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    df.to_csv('{}{}.csv'.format(csv_folder,data_path.split('raw/')[1]),index=False)
        
#%% check if there are new files
country = 'tunisia'
base_data_path ='./data/%s/raw/'% country
files_list = glob.glob('%s*.csv'% base_data_path)
for i in range(len(files_list)):
    files_list[i] = files_list[i].replace("\\","/") 
#%%  
    
#check for data log  
data_log = glob.glob('%sdata_log.txt'% base_data_path)
if len(data_log)==0:
    f = open('%sdata_log.txt'% base_data_path,'w')
    for i in range(len(files_list)):
        data_path = files_list[i].split('.csv')[0]
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


