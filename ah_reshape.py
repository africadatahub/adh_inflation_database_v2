# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 10:53:32 2022

@author: heiko
"""

import pandas as pd, numpy as np
from datetime import date
import matplotlib.pyplot as plt

today = date.today()
df = pd.read_csv('./outputs/ckan/{}_combined_imf_database.csv'.format(today))
indicators = df.loc[:,['Indicator.Name','Indicator.Code']].drop_duplicates()
indicators = dict(zip(indicators['Indicator.Code'], indicators['Indicator.Name']))
df_1 = pd.read_csv('./outputs/ckan/bk/reshape_template.csv')

df_1['date'] = df_1['date'].apply(lambda x: x.split('T')[0])

#%%
def reshape_db(df,country):
    
    df = df.drop(columns=['Indicator.Name','_id'])
    df = df[df.Country == country]
    country = df.Country.drop_duplicates().to_list()
    iso = df.Geography.drop_duplicates().to_list()
    df_2 = df.drop(columns=['Geography','Country'])
    df_2 = df_2.transpose()
    df_2.columns = df_2.iloc[0]
    df_2 = df_2.drop(['Indicator.Code'])
    
    
    df_2 = df_2.rename_axis('date').reset_index()
    df_2.insert(0,'Country',country[0])
    df_2.insert(0,'iso_code',iso[0])
    
    return df_2
#%%
countries = df.Country.drop_duplicates().to_list()
df_db = pd.DataFrame()
#countries = countries[0:3]
for country in countries:
    df_2 = reshape_db(df,country)
    df_db = pd.concat([df_db,df_2])
df_db.columns.values[15] = "Insurance"
cols = df_db.columns.to_list()
cols = cols[3:]
for col in cols:
    df_db[col] = df_db[col].astype(float)
    
    
#%%
#%% compare
check = True
df_diff = pd.DataFrame() 
if check == True:
    for k in range(len(countries)):
        country = countries[k]
        if '_' in country:
            c = country.split('_')
            c = [i.capitalize() for i in c]
            country = ' '.join(c)
        else:
            country = country.capitalize()
        if country =='Gambia':
            country = 'The Gambia'
            
        df_orig = df_1[df_1.Country==country]
        df_new = df_db[df_db.Country==country]
        
        
        df_orig = df_orig.drop(columns=['_id', 'iso_code', 'Country'])
        df_new = df_new.drop(columns=['iso_code', 'Country','Insurance'])
 
      
        vals = df_orig.columns.to_list()
        vals = vals[1:]
        for i in range(len(vals)):
            val = vals[i]
            #ax = df_orig.plot(x='date',y=val,label='original')
            #df_new.plot(ax=ax,x='date',y=val)
            #plt.title('{}, {}, {}'.format(country,indicators[val],val))
            
            z_df_orig = df_orig.loc[:,['date',val]]
            z_df_new = df_new.loc[:,['date',val]]
            z_df = pd.merge(z_df_orig,z_df_new,how='left',on='date',suffixes=('_old','_new'))
            z_df['diff'] = z_df.iloc[:,1] - z_df.iloc[:,2]
            z_df = z_df[z_df['diff'] != 0]
            z_df = z_df[z_df['diff'].notnull()]
            
            # check that it is not a rounding error
            z_df['diff'] = z_df['diff']*z_df['diff']
            z_df['diff'] = np.sqrt(z_df['diff'])
            z_df = z_df[z_df['diff'] >= 0.1]
            z_df['Country'] = country
            z_df['indicator'] = indicators[val]
            df_diff = pd.concat([df_diff,z_df])
#%%





