# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 15:52:31 2022
This is a debugging script for when you want to compare data for a specific country between IMF and what we have gathered
@author: heiko
"""

import pandas as pd, numpy as np
from datetime import date, datetime, timedelta
import glob, shutil, os
import matplotlib.pyplot as plt

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

def use_imf(df_imf,df):
    cols = df_imf.columns.to_list()
    res = cols[4:]
    cols = cols[0:4]
    res = [get_last_date_of_month(int(val.split('M')[0]),int(val.split('M')[1])) for val in res]
    cols = cols + res
    df_imf.columns = cols
    df_imf = df_imf.round(2)
    df_imf = df_imf.drop(columns=['Attribute'])
    df_imf = df_imf.set_index(['Country','Indicator.Name','Indicator.Code'])
    df = df.set_index(['Country','Indicator.Name','Indicator.Code'])
    df.update(df_imf)
    return df

#%%
imf = False
data_path= './outputs/ckan/'
df_bk = pd.read_csv('{}/bk/africainflationdata.csv'.format(data_path))

country = 'zimbabwe'
country2 = country.capitalize()
df_bk = df_bk[df_bk.Country==country2]

file = glob.glob('./outputs/imf/*.xlsx')
df_imf = pd.read_excel(file[0])
df_imf = df_imf[df_imf.Country==country2]

man_comp = True
if man_comp==True:
    df_imf = df_imf.sort_values(by=['Indicator.Name'])
    df_bk = df_bk.sort_values(by=['Indicator.Name'])
    df_imf = df_imf.set_index(['Indicator.Name'])
    df_bk = df_bk.set_index(['Indicator.Name'])
#%%

df = pd.read_csv('./data/algeria/csv/algeria_output.csv')
df = df.loc[:,['Indicator.Name','Indicator.Code']]
df = pd.merge(df,df_bk,how='left',on=['Indicator.Name','Indicator.Code'])
cols = df.columns.to_list()
cols = cols[4:]
df['_id'] = 0
cols = ['Indicator.Name','Indicator.Code','Country','_id','Geography']+cols
df = df.loc[:,cols]

if imf== True:
    df = use_imf(df_imf,df)

df.to_csv('./data/{}/csv/{}_output.csv'.format(country,country),index=False)


#%% rebase experiment - as expected, rebase does not affect inflation percentage
'''
jan_21 = 11
feb_21 = 37
mar_21 = 83
jun_21 = 18
jul_21 = 23

jan_22 = 5
feb_22 = 27
mar_22 = 43
jun_22 = 98
jul_22 = 13

#%using mar_21 as base

jan_21_a = jan_21/mar_21
feb_21_a = feb_21/mar_21
mar_21_a = mar_21/mar_21
jun_21_a = jun_21/mar_21
jul_21_a = jul_21/mar_21

jan_22_a = jan_22/mar_21
feb_22_a = feb_22/mar_21
mar_22_a = mar_22/mar_21
jun_22_a = jun_22/mar_21
jul_22_a = jul_22/mar_21

# using jan_21
jan_21_b = jan_21/jan_21
feb_21_b = feb_21/jan_21
mar_21_b = mar_21/jan_21
jun_21_b = jun_21/jan_21
jul_21_b = jul_21/jan_21

jan_22_b = jan_22/jan_21
feb_22_b = feb_22/jan_21
mar_22_b = mar_22/jan_21
jun_22_b = jun_22/jan_21
jul_22_b = jul_22/jan_21

def inflation(A,B):    
    inf_a = ((B-A)/A)*100
    return inf_a

jan_inf_a = inflation(jan_21_a,jan_22_a)
feb_inf_a = inflation(feb_21_a,feb_22_a)
mar_inf_a = inflation(mar_21_a,mar_22_a)
jun_inf_a = inflation(jun_21_a,jun_22_a)
jul_inf_a = inflation(jul_21_a,jul_22_a)

jan_inf_b = inflation(jan_21_b,jan_22_b)
feb_inf_b = inflation(feb_21_b,feb_22_b)
mar_inf_b = inflation(mar_21_b,mar_22_b)
jun_inf_b = inflation(jun_21_b,jun_22_b)
jul_inf_b = inflation(jul_21_b,jul_22_b)
'''