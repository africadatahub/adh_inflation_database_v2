# adh_inflation_database_v2

ADH African Country Inflation Steps
Introduction
The IMF has a great inflation database, but it relies on countries to provide their latest data to the IMF, and as such, it can be temporarily out of date. The ADH African Country Inflation database will keep the IMF inflation database up to date for African countries by scraping data from individual countries' websites as soon as they release their data and combining it with the latest IMF data.

The py scripts for the ADH Inflation Database were run in Spyder.

Steps

Set up a local directory for C:\Users\adh_inflation_database_v2.  

Go to the  IMF Consumer Price Index Data Portal https://data.imf.org/en/datasets/IMF.STA:CPI  and click the download button. The name of the download file will be similar to this: 
dataset_2025-11-24T08_30_48.624811811Z_DEFAULT_INTEGRATION_IMF.STA_CPI_5.0.0.csv,  only the date will change. Save the download, which will be about 347 MB,   to \adh_inflation_database_v2\data\imf\raw , and delete the previous file in this folder.  Only the most recent download should be kept in this folder.

Run adh_prep_imf_New25.py in the folder  \adh_inflation_database_v2\


This will extract CPI_day-month-year hr-min-sec_NewtimeSeries_africa.xlsx from the world data in the download and save African CPI time series to the folder: .\adh_inflation_database_v2\outputs\imf\
