# Province-Anomaly-RAN-KPIs-Detection-
The goal of this project is to find the abnormal behaviors in different provinces as soon as possible to reduce costs. 


## Goal
As the Telecom is a live network and may face any abnormality at any time, it is important to find such aomalies as soon as possible to fix the case. </b>
So, there is important to check abnormalites and detect them every hour (which is the minimum required time to get new data from mobile communication antennas (sites). 
I have provided an algorithm to check the main radio KPIs of sites (2G/3G/4G) as soon as recieving the new data and find the abnormal behaviour and make responsible teams aware of this case. Therfore the improvment process will began quickly. 

## Method
In this project the abnormal behaviours will be detected by applying statistical analysis inclusing below steps. </b>
Please consider the input files are just fake maid examples to use as a template and should be customized by you to use the script. </b>
- Exporting the data from database or tools.
- Checking the validation of last satats based on your tools policy (I counted the number of incomming cells measurement).
- Calculating the Median of each KPI per province and based on the trend in holidays and working days.
- Applying different targets per KPI per province to provided meadian (you can customize this part in "target - Percentage.xlsx" input file.
- If you are using fixed targets for some KPIs, you can cusyomize the relevant script part and the input "target -fixed.xlsx".
- Calculating the 'Aging' for each degradation.
- To check the root cause of degradation , you can use "RootCause.xlsx" input file, which has embeded in the script file.

## Input
- province_degradation_hourly.xlsx : consists of hourly values of different KPIs per province.
- 2G_Province_Daily_.xlsx : consists of 2G Cells number which is used as a reference to check missing. There are similar files for other technologies.
- target - Percentage.xlsx : is used to customize degradation. It shows up to how much degradation from target is acceptable.
- target - fixed.xlsx : is used when you want to apply a fix value as a target per KPI per province.
- RootCause.xlsx : is used to categorize degradation based on their rootcause and responsible team. 



## Output
Output file is a .csv file consists of name of province, time of occured issue, Aging of it and root cause. </b>
Example of Output file:

<img src= />
