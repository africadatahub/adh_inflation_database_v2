# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 13:46:29 2022

Zimbabwe rebased their data from 2019 to 2020
Ordinarily this should not affect inflation percentage, unless they have changed the basket contents
Need to figure out which rebase IMF uses...
@author: heiko
"""

import pandas as pd

def prep(df):
    df = df.drop(columns=['Unnamed: 0'])
    df = df.set_index('Unnamed: 1')
    df = df.transpose()
    cols = df.columns.to_list()
    for col in cols:
        df[col] = df[col].str.replace(',','')
        df[col] = df[col].astype(float)
    return df

def prep_new(df,year):
    df = df.drop(columns=['Unnamed: 12'])
    df = df.set_index('Unnamed: 0')
    df = df.transpose()
    df = df.add_suffix(year)
    df = df.reset_index()
    df = df.rename(columns={'index':'Indicator.Name'})
    return df


def inflation(df_old,df_new,year):
    df_old = df_old.loc[:,df_new.columns]
    df = ((df_new-df_old)/df_old)*100
    df = df.round(2)
    df = df.add_suffix(year)
    df = df.reset_index()
    df = df.rename(columns={'index':'Indicator.Name'})
    return df

#%%
'''

#df = pd.read_csv('./data/zimbabwe/raw/tabula-Blended_CPI_OCT_22_rebase_2020.csv')
df = pd.read_csv('./data/zimbabwe/raw/tabula-CPI_07_2022_Combined_rebase_2019.csv')

df_2020 = df.iloc[0:12,:]
df_2021 = df.iloc[12:24,:]
df_2022 = df.iloc[24:,:]
df_2020 = prep(df_2020)
df_2021 = prep(df_2021)
df_2022 = prep(df_2022)

df_2021_inf = inflation(df_2020,df_2021,'_2021')
df_2022_inf = inflation(df_2021,df_2022,'_2022')

df_inf = pd.merge(df_2021_inf,df_2022_inf,how='left',on='Indicator.Name')
#df_inf.to_csv('./data/zimbabwe/csv/blended_rebase_2020_inflation.csv')
df_inf.to_csv('./data/zimbabwe/csv/rebase_2019_inflation.csv')
'''


#%% from now we use monthly reports from reerve bank, which have already determined the percentage change. 
# note, reserve bank data is incorrectly columned from Jan-July 2020, so just ignore

df = pd.read_csv('./data/zimbabwe/raw/tabula-Monthly_Economic_Review_September_2022.csv')
df_2020 = df.iloc[1:12,:]
df_2021 = df.iloc[13:25,:]
df_2022 = df.iloc[26:,:]

df_2020 = prep_new(df_2020,'_2020')
df_2021 = prep_new(df_2021,'_2021')
df_2022 = prep_new(df_2022,'_2022')

df_inf = pd.merge(df_2021,df_2022,how='left',on='Indicator.Name')
df_inf.to_csv('./data/zimbabwe/csv/tabula-Monthly_Economic_Review_September_2022_rebase_2019_inflation.csv')