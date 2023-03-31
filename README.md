# Province-Anomaly-RAN-KPIs-Detection-
The goal of this project is to find abnormal behaviors in different provinces as soon as possible to reduce costs. 


## Goal
As Telecom is a live network and may face any abnormality at any time, it is important to find such anomalies as soon as possible to fix the case. </br>
So, it is important to check abnormalities and detect them every hour (which is the minimum required time to get new data from mobile communication antennas (sites)). 
I have provided an algorithm to check the main radio KPIs of sites (2G/3G/4G) as soon as receiving the new data and find the abnormal behaviour and make responsible teams aware of this case. Therefore the improvement process will begin in a short period time after detection. 

## Method
In this project, abnormal behaviours will be detected by applying statistical analysis including the below steps. </br>
Please consider the excel file values are not real and are just examples that can be used as a template and should be customized by you to use the script. </br>
- Exporting the data from databases or tools.
- Checking the validation of the last stats based on your tools policy (I count the number of incoming cells measurement).
- Calculating the Median of each KPI per province based on the trend in holidays and working days.
- Applying different targets per KPI per province to provide a median (you can customize this part in the "target - Percentage.xlsx" input file.
- If you are using fixed targets for some KPIs, you can customize the relevant script part and the input "target -fixed.xlsx".
- Calculating the 'Aging' for each degradation.
- To check the root cause of degradation, you can use the "RootCause.xlsx" input file, which has embedded in the script file.

## Input
- province_degradation_hourly.xlsx : consists of hourly values of different KPIs per province.
- 2G_Province_Daily_.xlsx : consists of 2G Cells number which is used as a reference to check missing. There are similar files for other technologies.
- target - Percentage.xlsx : is used to customize degradation. It shows up to how much degradation from the target is acceptable.
- target - fixed.xlsx : is used when you want to apply a fixed value as a target per KPI per province.
- RootCause.xlsx : is used to categorize degradation based on their root cause and responsible team. 



## Output
The output file is a '.csv' file consisting of the name of province, the time of occurred issue, Aging of it and root cause. </b>
Example of an output file:

<img src=https://github.com/HadisAB/Province-Anomaly-RAN-KPIs-Detection-/blob/main/Table.jpg />
