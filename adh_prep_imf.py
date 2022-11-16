# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 10:54:20 2022

@author: heiko
"""
import pandas as pd
import glob, shutil

print('Running prep_imf...')

data_path= './data/imf/raw/'
file = glob.glob(data_path+'*')
file_name = file[0].split('\\')[1].split('.')[0]
df_imf =pd.read_csv('{}{}.zip'.format(data_path,file_name))
df_imf_h = df_imf.head(1000)

iso_codes = pd.read_csv('./data/imf/map_imf_iso_code.csv')
iso = iso_codes.imf_code.drop_duplicates().to_list()
iso_codes = iso_codes.rename(columns={'imf_code':'Country Code','iso_code':'Geography'})
iso_codes = iso_codes.drop(columns='country')
df_imf = df_imf[df_imf['Country Code'].isin(iso)]
df_imf = pd.merge(df_imf,iso_codes,how='left',on='Country Code')
countries_imf_af = df_imf['Country Name'].drop_duplicates().to_list()

# template needs to come from previous output
#data_path_temp= './data/imf/'
#df_template = pd.read_csv('{}combined_imf_template.csv'.format(data_path_temp))
#data_path_temp = './outputs/imf/'
#file = glob.glob(data_path_temp + '*')[0]
#df_template = pd.read_excel(file)
df_imf = df_imf[df_imf.Attribute=='Value']

cols = [col for col in df_imf.columns if 'M' in col]
cols = ['Country Name', 'Country Code','Indicator Name','Indicator Code','Attribute'] + cols
df_imf = df_imf.loc[:,cols]
cols = [col for col in df_imf.columns if '20' in col]
cols = ['Country Name', 'Country Code','Indicator Name','Indicator Code','Attribute'] + cols
df_imf = df_imf.loc[:,cols]
cols = df_imf.columns.to_list()
keep = cols[0:5]+cols[101:]
df_imf= df_imf.loc[:,keep]
#df_imf = df_imf.drop_duplicates(keep=False)

codes = pd.read_csv('./data/codeList.csv')
inds = codes['Indicator.Code'].to_list()
df_imf = df_imf[df_imf['Indicator Code'].isin(inds)]



#%% fix the country names
#countries_template = df_template['Country'].drop_duplicates().to_list()

#df_template['Country'][df_template['Country'].str.contains('Ivoire',case=False)==True] = "Côte d'Ivoire"
df_imf['Country Name'][df_imf['Country Name'].str.contains('São Tomé and Príncipe',case=False)==True] = "São Tomé and Príncipe"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Comoros',case=False)==True] = "Comoros"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Gambia',case=False)==True] = "The Gambia"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Ethiopia',case=False)==True] = "Ethiopia"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Egypt',case=False)==True] = "Egypt"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Equatorial',case=False)==True] = "Equatorial Guinea"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Eswatini',case=False)==True] = "Eswatini"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Congo, Dem. Rep. of the',case=False)==True] = "Democratic Republic of the Congo"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Central African Rep.',case=False)==True] = "Central African Republic"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Congo, Rep. of',case=False)==True] = "Republic of Congo"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Mauritania',case=False)==True] = "Mauritania"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Mozambique',case=False)==True] = "Mozambique"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Lesotho',case=False)==True] = "Lesotho"

df_imf['Country Name'][df_imf['Country Name'].str.contains('Madagascar',case=False)==True] = "Madagascar"
df_imf['Country Name'][df_imf['Country Name'].str.contains('Tanzania',case=False)==True] = "Tanzania"
df_imf['Country Name'][df_imf['Country Name'].str.contains('South Sudan',case=False)==True] = "South Sudan"

df_imf = df_imf.rename(columns={'Country Name':'Country','Indicator Name':'Indicator.Name','Indicator Code':'Indicator.Code'})
df_imf = df_imf.drop(columns=['Country Code'])
df_imf = df_imf.drop_duplicates(keep=False)

#%% drop percentage change, previous year

df_imf['Indicator.Name'] = df_imf['Indicator.Name'].str.replace(', Percentage change, Previous year','')


#%% move old output files to backup

output_path = './outputs/imf/'
files = glob.glob(output_path+'*.xlsx')
for file in files:
    shutil.move(file,'./outputs/imf/bk/')

#%%
df_imf.to_excel('./outputs/imf/{}_africa.xlsx'.format(file_name),index=False)

#%% find missing data
'''
def find_missing(df_im,df_temp,country):
    df_im = df_im[df_im['Country Name'] == country]
    df_temp = df_temp[df_temp.Country==country]
    if len(df_im) != len(df_temp):
        print('found it: {}'.format(country))
    return df_im, df_temp
for i in range(len(countries_imf_af)):
    print(i)
    df_im, df_temp = find_missing(df_imf,df_template,countries_imf_af[i])

df_im, df_temp = find_missing(df_imf,df_template,countries_imf_af[34])
'''



