# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 12:18:05 2022

@author: heiko
"""

import pandas as pd, numpy as np
from datetime import date, datetime, timedelta
import glob, shutil, os
import matplotlib.pyplot as plt


def prep(df, c_bk):
    cols = df.columns.to_list()
    cols = cols[5:]
    df = df.astype(str)
    for col in cols:
        #print(col)
        df[col] = df[col].str.replace(',','.')
        df[col] = df[col].str.replace('%','')
        df[col] = df[col].astype(float)
        df[col] = df[col].round(2)
    df = df.set_index(['Geography'])
    df.update(c_bk)
    df = df.reset_index()
    df = df.set_index(['Country','Indicator.Name'])
    df = df.drop(columns = ['_id', 'Geography'])
    return df

def tidy_up(country):
    # save this in csv folder
    bk_folder = "C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\data\\%s\\csv\\bk\\"% country
    # create csv_folder folder
    if not os.path.exists(bk_folder):
        os.makedirs(bk_folder)
    # move everything other than translation template and output to bk

    files = glob.glob("C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\data\\%s\\csv\\*.csv"% country)
    keep = ['output','template']
    files = [item for item in files if not keep[0] in item]
    files = [item for item in files if not keep[1] in item]
    
    for file in files:
        filename = file.split('\\')[-1]
        shutil.move(file,os.path.join(bk_folder, filename))

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

def reshape_db(df,country):
    
    df = df.drop(columns=['Indicator.Name','_id','index'])
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
# first get the most recent ckan dataset ie the one we uploaded last

data_path= './outputs/ckan/'
file = glob.glob(data_path+'*.csv')
#file = ['./outputs/ckan/bk/template.csv']
df_ckan = pd.read_csv(file[0])
df_ckan_orig = pd.read_csv(file[0])
df_ckan = df_ckan[df_ckan['Indicator.Name']!='Insurance and Financial Services']

# check names
df_bk = pd.read_csv('{}/bk/africadata3.csv'.format(data_path))
c_bk = df_bk.loc[:,['Country','iso_code']].drop_duplicates()
df_ckan = df_ckan.set_index(['Geography'])
c_bk = c_bk.rename(columns={'iso_code':'Geography'})

c_bk = c_bk.set_index(['Geography'])
df_ckan.update(c_bk)  
df_ckan = df_ckan.reset_index()

c_ckan = df_ckan.loc[:,['Country','Geography']].drop_duplicates()
df_ckan = df_ckan.set_index(['Country','Indicator.Name','Indicator.Code'])

#%% now update with the latest IMF dataset

file  = glob.glob('./outputs/imf/*.xlsx')
df_imf = pd.read_excel(file[0])
cols = df_imf.columns.to_list()
res = cols[4:]
cols = cols[0:4]
res = [get_last_date_of_month(int(val.split('M')[0]),int(val.split('M')[1])) for val in res]
cols = cols + res
df_imf.columns = cols
df_imf = df_imf.round(2)
df_imf = df_imf.drop(columns=['Attribute'])
c_imf = df_imf.Country.drop_duplicates().to_list()
df_imf = df_imf.set_index(['Country','Indicator.Name','Indicator.Code'])
cols = cols[-12:]
# lets only update the last year
df_imf = df_imf.loc[:,cols]


#%% check names
c_ckan = c_ckan.Country.drop_duplicates().to_list()
filter_set = set(c_ckan)
bad_names = [x for x in c_imf if x not in filter_set]

if len(bad_names) > 0:
    print('##################################')
    print('There is an issue with names')
    print('##################################')
    print(' ')
    print('press any key to exit')
    ans = input('')
    exit()
del c_ckan, c_imf
#%% get latest columns
# update with latest dates
ckan_cols = df_ckan.columns.to_list()
ckan_cols = ckan_cols[-5:]
df_cols = df_imf.columns.to_list()
df_cols = df_cols[-5:]
cols = [x for x in df_cols if x not in ckan_cols]
if cols:
    for col in cols:
        df_ckan[col] = ''


#%% update with latest IMF
df_ckan.update(df_imf)  


#%% find which countries we have data for

countries = os.listdir('./data/')
countries.remove('imf')
#countries.remove('burundi')
#countries.remove('nigeria')
countries.remove('codeList.csv')
countries.remove('imf_country_codes.pdf')
countries.remove('inflationdatacountrylist.csv')
#countries = countries[0:5]
# for each country, we want to get the output and clean up the data directory
for country in countries:
    print(country)
    #country = 'burkina_faso'
    df = pd.read_csv('./data/%s/csv/%s_output.csv'%(country,country))
    df = prep(df,c_bk)
    # update with latest dates
    ckan_cols = [col for col in df_ckan.columns if '2022' in col]
    df_cols = [col for col in df.columns if '2022' in col]
    cols = [x for x in df_cols if x not in ckan_cols]

    if cols:
        for col in cols:
            df_ckan[col] = ''
    tidy_up(country)
    df_ckan.update(df)
   

#%% save  
df_ckan = df_ckan.reset_index()
# move old output files to backup
#country = 'ckan'
full_path = "C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\outputs\\ckan\\"
full_path_bk = "C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\outputs\\ckan\\bk\\"
#output_path = './outputs/{}/'.format(country)

files = glob.glob(full_path+'*.csv')
for file in files:
    filename = file.split('\\')[-1]
    shutil.move(file,os.path.join(full_path_bk, filename))
today = date.today()
df_ckan.to_csv('./outputs/ckan/{}_combined_imf_database.csv'.format(today),index=False)            

#%% reshape
countries = df_ckan.Country.drop_duplicates().to_list()
df_db = pd.DataFrame()
#countries = countries[0:3]
for country in countries:
    df_2 = reshape_db(df_ckan,country)
    df_db = pd.concat([df_db,df_2])
#df_db.columns.values[15] = "Insurance"
cols = df_db.columns.to_list()
cols = cols[3:]
for col in cols:
    try:
        df_db[col] = df_db[col].astype(float)
    except:
        print('could not convert {} to float'.format(col))
df_db.to_csv('./outputs/ckan/{}_reshaped_imf_database.csv'.format(today),index=False)

#%% power_BI

df = df_ckan.drop(columns=['Indicator.Code','index','_id'])
df = df.rename(columns={'Indicator.Name':'indicator','Country':'country','Geography':'iso_code'})
df_2 = pd.melt(df,id_vars=['country','indicator','iso_code'], var_name=['date'])
df_2 = df_2.dropna()

df_2['value'] = df_2['value'].astype(float)

df_2.to_csv('./outputs/ckan/power_BI.csv',index=False)

#%% compare
check = False

if check == True:
    indicators = df_ckan.loc[:,['Indicator.Name','Indicator.Code']].drop_duplicates()
    indicators = dict(zip(indicators['Indicator.Code'], indicators['Indicator.Name']))
    df_diff = pd.DataFrame() 
    df_1 = pd.read_csv('./outputs/ckan/bk/reshape_template.csv') #change to previous db once this has been run for a month
    df_1['date'] = df_1['date'].apply(lambda x: x.split('T')[0])
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

    df_diff.to_csv('./outputs/ckan/{}_errors.csv'.format(today),index=False)
#%%



#%% compare
'''
check = False
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
            
        df_1 = df_ckan_orig[df_ckan_orig.Country==country]
        df_2 = df_ckan[df_ckan.Country==country]
        
        df_1 = df_1.drop(columns=['_id', 'Geography', 'Country','Indicator.Code'])
        df_2 = df_2.drop(columns=['_id', 'Geography', 'Country','Indicator.Code'])
        
        # compare apples with apples
        vals1 = df_1['Indicator.Name'].to_list()
        vals2 = df_2['Indicator.Name'].to_list()
        
        vals1 = set(vals1)
        vals = vals1.intersection(vals2)
        vals = list(vals)
        
        df_1 = df_1[df_1['Indicator.Name'].isin(vals)]
        df_2 = df_2[df_2['Indicator.Name'].isin(vals)]
        
        df_1 = df_1.sort_values(by='Indicator.Name').reset_index(drop=True)
        df_2 = df_2.sort_values(by='Indicator.Name').reset_index(drop=True)
        df_1 = df_1.fillna(0)
        df_2 = df_2.fillna(0)
        df_1 = df_1.set_index(['Indicator.Name'])
        df_2 = df_2.set_index(['Indicator.Name'])
        df_1 = df_1.transpose()
        df_2 = df_2.transpose()
        df_3 = df_1 - df_2   
        df_3.plot()
        plt.title(country)
'''