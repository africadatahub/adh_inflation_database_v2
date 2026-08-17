# adh_inflation_database_v2

ADH African Country Inflation Steps
Introduction
The IMF has a great inflation database, but it relies on countries to provide their latest data to the IMF, and as such, it can be temporarily out of date. The ADH African Country Inflation database will keep the IMF inflation database up to date for African countries by scraping data from individual countries' websites as soon as they release their data and combining it with the latest IMF data.

The py scripts for the ADH Inflation Database were run in Spyder.

Steps

Step 1
Set up a local directory for C:\......\adh_inflation_database_v2.

The easiest way to setup the correct directory structure is the use the download zip option that can be accessed if you click on the green Code button.  The downloaded file will be titled adh_inflation_database_v2-main.zip

On you local drive setup sub-directories that are exactly the same us in this repo https://github.com/africadatahub/adh_inflation_database_v2

The main sub-directories for \adh_inflation_database_v2
C:\.....\adh_inflation_database_v2\data
C:\.....\adh_inflation_database_v2\outputs

Step 2
Download the Consumer Price Index (CPI) from the IMF Data Portal
Go to the  IMF Consumer Price Index Data Portal https://data.imf.org/en/datasets/IMF.STA:CPI and click the download button. The name of the download file will be similar to this: 
dataset_2025-11-24T08_30_48.624811811Z_DEFAULT_INTEGRATION_IMF.STA_CPI_5.0.0.csv,  only the date and time will change. 
Save the download, which will be about 347 MB, to \adh_inflation_database_v2\data\imf\raw , and delete the previous file in this folder.  Only the most recent download should be kept in this folder.

Step 3
Extract the African country CPI data from the IMF download by running adh_prep_imf_New25.py in the folder  \adh_inflation_database_v2\

This will extract a CPI Time Series Africa file with a name similar to this "CPI_day-month-year hr-min-sec_NewtimeSeries_africa.xlsx" from the world data in the download and will save African CPI time series automatically to the folder: \adh_inflation_database_v2\outputs\imf\

Step 4
Note the date of the IMF download and check African countries for more recent CPI data on the web.

Use the Notes.ods spreadsheet in .\adh_inflation_database_v2, to record the date of the IMF download, and also to document the dates of individual country updates.  The country CPI websites are checked to see if the have more recent CPI data than the IMF download.
The columns in Notes.ods are: Country, Language, Analysis Method, Publication_Order, latest IMF data, latest ADH country data, Country publication day, Date site checked, data on date checked, analysed, checked, Comments, Source.
The Country column only includes countries that are publishing CPI data on the web.  For the other countries, updates are received from the IMF. 
The Analysis Method column indicates if there is a py script to run the country update, or if a manual update is required.  
Before using the country links in the source column, it would be good to update the latest IMF data column by referring to CPI_day-month-year 08-30-48_NewtimeSeries_africa.xlsx to find the date of most recent CPI data for each country.
The Country publication day gives the date of each month when the country publishes CPI data.  Only check for updates after the date of publication.
The 3 columns: use the key 1  yes and 2 = no to indicate if there was updated 1) data on date checked, analysed (1 = yes data was manually entered or py script was used) , checked (1= yes the country output file has been checked against the source document for correctness) 
The Source column has links to country Consumer Price Index updates.  Once a country CPI update has been located (usually pdf or excel)  it must be downloaded and saved to, for example,  .\adh_inflation_database_v2\data\algeria\raw  
In this example, Algeria requires a manual update, and the Comments column has information where to find the CPI update table in the download. The file to which the CPI updates must be added is algeria_output.csv and this is in the  .\adh_inflation_database_v2\data\algeria\csv folder.  Save and close the country_output.csv file, in this case algeria_output.csv.
For countries where a py script is used for the update, such as Namibia, you would go to https://nsa.org.na/publications/ and download the update file Namibia-CPI-Month-Year-Excel-Tables-.xlsx from the Namibia Consumer Price Index Bulletin (NCPI) section.  Save this file to .\adh_inflation_database_v2\data\namibia\raw.  Then run adh_prep_namibia.py in the folder .\adh_inflation_database_v2.  This script will produce an output file Namibia-CPI-Month-Year-Excel-Tables-.csv in the folder \adh_inflation_database_v2\data\namibia\csv and the update will be in the last column, which must be copied and pasted to the last column of  namibia_output.csv, in the .\adh_inflation_database_v2\data\namibia\csv\ folder. Save and close the country_output.csv file.

Step 5
Combine the individual country updates with the  "CPI_day-month-year hr-min-sec_NewtimeSeries_africa.xlsx". This done by running the adh_create_inflation_db.py script in \adh_inflation_database_v2\

Change the following paths in adh_create_inflation_db.py so that they are correct in your local environment
Line 33  
bk_folder = "C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\data\\%s\\csv\\bk\\"% country
Line 39 
files = glob.glob("C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\data\\%s\\csv\\*.csv"% country)
Line 188
full_path = "C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\outputs\\ckan\\"
Line 189
full_path_bk = "C:\\Users\\heiko\\Documents\\Work\\OCL\\ADH\\Inflation\\adh_inflation_database_v2\\outputs\\ckan\\bk\\"

This will create 3 output files in \adh_inflation_database_v2\outputs\ckan\: 1) year-month-day_combined_imf_database.csv, 2) year-month-day_reshaped_imf_database.csv, and 3) power_BI.csv.  All 3 datasets found here contain the same data, but in different shapes to suit different applications.

Step 6
Update CKAN IMF Africa Inflation Database at https://ckan.africadatahub.org/dataset/imf-africa-inflation-database,  This Africa inflation database powers the ADH Inflation Observer (https://www.africadatahub.org/dashboards/inflation-observer) . 
Management rights will be needed to do the update.  For example to update the combined_imf_database.csv , click on that file name under Data and Resources, then on the combined_imf_database.csv page click on Edit Resource, the select clear upload, then upload the update file from \adh_inflation_database_v2\outputs\ckan\
END
