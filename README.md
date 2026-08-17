# adh_inflation_database_v2

ADH African Country Inflation Steps
Introduction
The IMF has a great inflation database, but it relies on countries to provide their latest data to the IMF, and as such, it can be temporarily out of date. The ADH African Country Inflation database will keep the IMF inflation database up to date for African countries by scraping data from individual countries' websites as soon as they release their data and combining it with the latest IMF data.

The py scripts for the ADH Inflation Database were run in Spyder.

Steps

Set up a local directory for C:\......\adh_inflation_database_v2.

The easiest way to setup the correct directory structure is the use the download zip option that can be accessed if you click on the green Code button.  The downloaded file will be titled adh_inflation_database_v2-main.zip

On you local drive setup sub-directories that are exactly the same us in this repo https://github.com/africadatahub/adh_inflation_database_v2

The main sub-directories for \adh_inflation_database_v2
C:\.....\adh_inflation_database_v2\data
C:\.....\adh_inflation_database_v2\outputs

Download the Consumer Price Index (CPI) from the IMF Data Portal
Go to the  IMF Consumer Price Index Data Portal https://data.imf.org/en/datasets/IMF.STA:CPI and click the download button. The name of the download file will be similar to this: 
dataset_2025-11-24T08_30_48.624811811Z_DEFAULT_INTEGRATION_IMF.STA_CPI_5.0.0.csv,  only the date and time will change. 
Save the download, which will be about 347 MB, to \adh_inflation_database_v2\data\imf\raw , and delete the previous file in this folder.  Only the most recent download should be kept in this folder.

Extract the African country CPI data from the IMF download by running adh_prep_imf_New25.py in the folder  \adh_inflation_database_v2\

This will extract a CPI Time Series Africa file with a name similar to this "CPI_day-month-year hr-min-sec_NewtimeSeries_africa.xlsx" from the world data in the download and will save African CPI time series automatically to the folder: \adh_inflation_database_v2\outputs\imf\
