# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 11:36:45 2022

A control script to run all of the prep data scripts

@author: heiko
"""
import os

auto = False
countries = os.listdir('./data/')
#countries.remove('ckan')
countries.remove('imf')
#countries.remove('nigeria')
#countries.remove('chad') 

# do manually
countries.remove('burundi')
countries.remove('eswatini')
countries.remove('malawi')
countries.remove('senegal')
countries.remove('sierra_leone')
countries.remove('togo')
countries.remove('zambia')
countries.remove('zimbabwe')

countries.remove('codeList.csv')
countries.remove('imf_country_codes.pdf')
countries.remove('inflationdatacountrylist.csv')
#%%

files = []
failed = []
for country in countries:
    files.append('adh_prep_%s.py'% country)

#files = ['adh_prep_imf.py']+files
#%%
if auto == True:
    for file in files:
        try:
            print(' ')
            print("running: {}".format(file))
            with open(file) as f:
                code = compile(f.read(), file, 'exec')
                exec(code)
            
        except:
            print('failed to execute {}'.format(file))
            failed.append(file)
else:
    i = 19
    file = files[i]

    try:
        print(' ')
        print("running: {}".format(file))
        with open(file) as f:
            code = compile(f.read(), file, 'exec')
            exec(code)
        
    except:
        print('failed to execute {}'.format(file))
        failed.append(file)