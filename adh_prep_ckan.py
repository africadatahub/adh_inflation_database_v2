# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 11:39:14 2022

Some countries dont have all of the options. This needs to be remedied - even if the row is blank

@author: heiko
"""

import pandas as pd
from datetime import date, datetime, timedelta
import glob, shutil, os

# get template
data_path= './outputs/ckan/'
file = glob.glob(data_path+'*.csv')
df = pd.read_csv(file[0])

df_k = df[df.Country=='Kenya']
ind_names = df_k[['Indicator.Name','Indicator.Code']]
countries = df.Country.drop_duplicates().to_list()
geos =  df.Geography.drop_duplicates().to_list()
'''
ind_names = ind_names.drop(11).reset_index(drop=True)
ind_names = ind_names.loc[:,['Indicator.Name','Indicator.Code']]
ind_code =  df['Indicator.Code'].drop_duplicates().to_list()

#ind_name =  df['Indicator.Name'].drop_duplicates().to_list()

extra = pd.DataFrame({'Indicator.Name':'Insurance and Financial Services','Indicator.Code':''},index=[0])
ind_names = pd.concat([ind_names,extra])
'''
#%% check each country
df_c = pd.DataFrame()
for i in range(len(countries)):
    df_1 = df[df.Country==countries[i]]
    df_2 = pd.merge(ind_names,df_1,how='left',on=['Indicator.Code'])
    df_2.Country = countries[i]
    df_2.Geography = geos[i]
    df_c = pd.concat([df_c,df_2])
    
df_c = df_c.drop(columns=['Indicator.Name_y'])
df_c = df_c.rename(columns={'Indicator.Name_x':'Indicator.Name'})
df_3 = df_c[df_c.Country=='Tanzania']

df_c.to_csv('./outputs/ckan/bk/template.csv',index=False)
