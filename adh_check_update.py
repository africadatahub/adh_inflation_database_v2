# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 14:24:09 2022

check if pandas update will overwrite with nans

@author: heiko
"""

import pandas as pd, numpy as np
import glob
data_path= './outputs/ckan/'
file = glob.glob(data_path+'*.csv')
#file = ['./outputs/ckan/bk/template.csv']
df_ckan = pd.read_csv(file[0])
df_ckan_orig = pd.read_csv(file[0])


df_imf = pd.read_excel('./outputs/imf/CPI_10-21-2022 13-39-28-71_timeSeries_africa.xlsx')

country = 'Niger'

df_ckan = df_ckan[df_ckan.Country==country]
df_imf = df_imf[df_imf.Country==country]

df_imf.columns = df_ckan.columns
df_ckan = df_ckan.set_index(['Country','Indicator.Name','Indicator.Code'])
df_imf = df_imf.set_index(['Country','Indicator.Name','Indicator.Code'])
df_ckan.update(df_imf)