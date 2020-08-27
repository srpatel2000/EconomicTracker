# Opportunity Insights Economic Tracker Data - San Diego 

This repository takes data gathered by the Opportunity Insights Economic Tracker and filters the data to be catered toward San Diego County. For a more wholistic view of the data and a data dictionary, please visit Opportunity Insight's Github [(link)](https://github.com/OpportunityInsights/EconomicTracker) As a current Data and Modeling Intern at SANDAG, this data is being used to track how the economy of the city + county of San Diego and the state of California is being impacted by COVID-19. 

### Folder Information 
#### data
This folder contains original data gathered by Opportunity Insights. For more information on these datasets please visit the link located in the introduction. 

#### data cleaning
- _raw_data_: This folder contains csv files that are needed to track the economy of San Diego, California, and the nation. I didn't use all the datasets from the data folder for 2 reason: (1) some datasets were already being used by SANDAG and (2) some datasets were not being updated often enough to draw important conclusions. 
- _temp_: This folder contains datasets from the week before the current date so that the _Data_Filtering_v1.py_ script can compare tables across updates and ensure that tables aren't drastically changing across weeks.
- _Data_Filtering_v1.py (OUTDATED)_: This python script filters Opportunity Insight's raw data to be catered towards San Diego county/city and California. It also transforms daily data to weekly data in order to match the other files and eliminate uneccessary use of space. Take a look at the script for more specific information on the functions and how the data is being filtered.
- _Data_Filtering_v2.py_: Opportunity Insight recently changed their Affinity Solutions datasets to be distributed daily rather than weekly, so this new python script takes that into account and transforms that data into weekly data. Along with this, it also completes the same tasks as _Data_Filtering_v1.py_.
- _Track The Recovery Data Filtering.ipynb_: This Jupyter Notebook was used to visualize any changes and finalize the python script files described above. This notebook can be used as a rough draft for editing the scripts because it can easily visualize changes occurring in the datasets. 
- _filtered_data_: This folder contains the filtered datasets after running the _Data_Filtering_v2.py_ script. 



