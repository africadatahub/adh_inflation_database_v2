# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 12:02:11 2023

Create a dynamic template for the indicator names

@author: heiko
"""

import tabula
import pandas as pd

data_path = './data/burkina_faso/raw/NOTE IHPC DECEMBRE 2022'
tables = tabula.read_pdf("{}.pdf".format(data_path), pages=(1), stream=True)

df = tables[0]
df = df.drop([0,1,2,3,4])
df = df.iloc[:,[0,-1]]
# remove rows with all nans
df = df[~df.isnull().all(axis=1)]

last = 'data'
df.columns = ['Indicator.Name',last]

#%% 

# pattern: nans occur in sentences, so each sentence needs to be joined on either side of the nan
# loop through indicator name to find nans
def get_template(df):
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
template = get_template(df)
#%%
# labels
df_labels = df.iloc[:,[0]]
#df_labels['Indicator.Name'] = df_labels['Indicator.Name'].str.replace('NAN','')
df['template'] = template
df = df.drop(columns=['Indicator.Name'])
df = df.dropna()
df_labels['template'] = template
df_labels = df_labels.dropna()
df_labels = df_labels.groupby(['template'])['Indicator.Name'].apply(' '.join).reset_index()

df = pd.merge(df,df_labels,how='left',on='template')
df = df.drop(columns='template')
df = df.loc[:,['Indicator.Name',last]]
df[last] = df[last].apply(lambda x: x.split(' ')[-1])


