# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 13:07:28 2022

Compare final output against original

@author: heiko
"""

import pandas as pd
import altair as alt
import glob
alt.renderers.enable('altair_viewer')
alt.data_transformers.disable_max_rows()
files = glob.glob('./outputs/ckan/*combined_imf_database.csv')
df = pd.read_csv(files[0])

#%% create filters

cols = [col for col in df.columns if '202' in col]
cols = ['Country', 'Indicator.Name'] + cols
df = df.loc[:,cols]
df_1 = pd.melt(df, id_vars=['Country', 'Indicator.Name'],var_name='date')
df_1 = df_1.rename(columns={'Indicator.Name':'indicator'})

locations = df_1.Country.unique()
locations = list(filter(lambda d: d is not None, locations)) # filter out None values
locations.sort() # sort alphabetically
demo_labels = locations.copy()

order = df_1['indicator'].drop_duplicates().to_list()
dates = df_1['date'].unique()
dates = list(filter(lambda d: d is not None, dates)) # filter out None values
dates.sort() # sort alphabetically

input_dropdown = alt.binding_select(options=locations, name='Select country',labels=demo_labels)
selection = alt.selection_single(fields=['Country'], bind=input_dropdown,init={'Country':'Kenya'})

input_dropdown_2 = alt.binding_select(options=dates, name='Select date',labels=dates)
selection_2 = alt.selection_single(fields=['date'], bind=input_dropdown_2,init={'date':dates[-1]})
#%%
# Create a centered title
title = alt.TitleParams('Inflation data for selected country', anchor='middle')
bar = alt.Chart(df_1,title=title).mark_bar().encode(
x = alt.X('value:Q',title='value'), # :O tells altair that the data is ordinal


y = alt.Y('indicator:N',title='Indicator',sort=order), 
tooltip=['value']
)

text = bar.mark_text(
    align='left',
    baseline='middle',
    dx=3  # Nudges text to right so it doesn't appear on top of the bar
).encode(
    text='value:Q'
)

graph = (bar + text).properties(
    width=800,
    height=600  
).add_selection(
    selection, selection_2
).transform_filter(
    selection & selection_2
)  

graph.show()




