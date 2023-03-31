# -*- coding: utf-8 -*-
"""
@author: hadis.ab

"""

import glob
import os
from ftplib import FTP
import re
import time
from datetime import datetime
import pandas as pd
#import pysftp
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import seaborn as sns
import copy
import shutil
import keyring
import warnings
warnings.filterwarnings("ignore")

#thresholds
target_threshold_decreasing=95
target_threshold_increasing=5
min_cells_percentage=0.8 #at least this percentage of cells is needed

'''Phase1: Define main required directories'''
main_directory=r'E:\Scripts\Anomaly_Province\\' #you can select a root for this project which contains all input and outputs
main_directory_DB=r'E:\Scripts\Anomaly_Province\DataBase\\' # the location of hourly inputs
main_directory_Output=r'E:\Scripts\Anomaly_Province\Outputs\\' # Output location

os.chdir(main_directory)
excel_lists=os.listdir(main_directory)
today=datetime.date.today()
yesterday = datetime.date.today()-datetime.timedelta(days=1)



#%% Reading the input files


os.chdir(main_directory)    

#------------
target_fixed=pd.read_excel("target -fixed.xlsx")

RootCause=pd.read_excel("RootCause.xlsx")


# reading the stats from our DataBase
os.chdir(main_directory_DB)  
excel_lists=os.listdir(main_directory_DB)
max_reports_in_DB=24*100

max_needed_stats_target=24*60#consider 2 months
excel_lists=excel_lists[-max_needed_stats_target:]
# if len(excel_lists)>max_reports_in_DB:
#     os.remove(excel_lists[0])
#     del excel_lists[0]
hourly_province_temp=[]      
for csv_file in excel_lists:
    print(csv_file)

    loc=main_directory_DB +csv_file
    temp=pd.read_csv(loc, header=1, delimiter=',',thousands=',')
    #temp=temp.rename(columns={'4G_Payload_DL_MByte_IR(MB)':'4G_Payload_PDCP_DL_Mbyte_IR(MB)', '4G_Payload_UL_MByte_IR(MB)':'4G_Payload_PDCP_UL_Mbyte_IR(MB)'})
    hourly_province_temp.append(temp)

os.chdir(main_directory)  
hourly_province_total=pd.concat([name for name in hourly_province_temp])


hourly_province_total=hourly_province_total.drop_duplicates(subset=['Time','Province'], keep='last')


#needed data based on Time
desired_hours=60 # last needed hours to be chcekd
desired_hours_graph=desired_hours+5
Total_hours=hourly_province_total['Time'].unique()
if len(Total_hours)<desired_hours:
    desired_hours=len(Total_hours)
desired_hours_list=Total_hours[-desired_hours:]
desired_hours_list_graph=Total_hours[- desired_hours_graph:]
hourly_province=hourly_province_total[hourly_province_total['Time'].isin(desired_hours_list)]
hourly_province_total.to_csv('total_stats.csv')
hourly_province_graph=hourly_province_total[hourly_province_total['Time'].isin(desired_hours_list_graph)]
#----------------
#os.remove(filename)
os.chdir(main_directory)  
target_percentage=pd.read_excel("target - Percentage.xlsx")

today=datetime.date.today()
end_desired_dates_target=today - datetime.timedelta(days=4)#4 bud bekhatere state kam
#end_desired_dates_target=datetime.date(2023, 3, 24)
hourly_province_total['Time']=pd.to_datetime(hourly_province_total['Time'])

target_province=hourly_province_total[hourly_province_total['Time'].dt.date<end_desired_dates_target]

province_list=['ALBORZ',	'ARDEBIL',	'BUSHEHR']
cell_num_2g='2G_Province_Daily_.csv'
CELLS_2G=pd.read_csv(cell_num_2g[-1], header=1, delimiter=',',thousands=',')[['Province','2G_CELLS_IR(#)']]
os.remove(cell_num_2g[-1])
cell_num_3g='3G_Province_Daily_.csv'
CELLS_3G=pd.read_csv(cell_num_3g[-1], header=1, delimiter=',',thousands=',')[['Province','3G_CELLS_IR(Number)']]
os.remove(cell_num_3g[-1])
cell_num_fdd='4G_Province_Daily_.csv'

CELLS_4G=pd.read_csv(cell_num_fdd[-1], header=1, delimiter=',',thousands=',')[['Province','4G_CELLS_IR(#)']]
os.remove(cell_num_fdd[-1])
cell_num_tdd='4GTDD_Province_Daily_.csv'

CELLS_TDD=pd.read_csv(cell_num_tdd[-1], header=1, delimiter=',',thousands=',')[['Province','TDD_4G Cell Number(#)']]
os.remove(cell_num_tdd[-1])

CELLS_2G=CELLS_2G[CELLS_2G['Province'].isin(province_list)].sort_values(by=['Province'])

cells_target=pd.merge(CELLS_2G,CELLS_3G, on='Province', how='left')
cells_target=pd.merge(cells_target,CELLS_4G, on='Province', how='left')
cells_target=pd.merge(cells_target,CELLS_TDD, on='Province', how='left')


# loc=r"CellCFG-4GFDD_.csv"
# data_maps_cellcfg=pd.read_csv(loc, header=1)
# os.remove(loc)


for counts in cells_target.columns[1:]:
    cells_target['target1']=cells_target[counts]*0.95
    cells_target['target2']=cells_target[counts]-150
    cells_target[counts]=cells_target[['target1','target2']].max(axis=1)
    del cells_target['target1'],cells_target['target2']



#changing the types in target province
target_province.dtypes
increasing_KPIs=[ '2G_TCH_AVAILABILITY_IR(%)',
       '3G Cell_Avail_Sys_IR(%)', '4G_CELL_AVAIL_SYS_IR', '2G_CSSR_IR(%)',
       '3G_CSSR_CS_IR(%)', '3G_CSSR_PS_IR(%)', '4G_CSSR_PS_IR(%)'       ]
decreasing_KPIs=['2G_DCR_IR(%)', '4G_ERAB_DROP_RATE_IR(%)', '3G_RAB_Drop_Rate_CS_IR(%)',
       '3G_RAB_Drop_Rate_HS_IR(%)','3G_HS Frame Loss Ratio_IR(%)']



KPIs_2G=['2G_CELLS_IR(#)','2G_TCH_AVAILABILITY_IR(%)','2G_DCR_IR(%)','2G_CSSR_IR(%)'
       ]
KPIs_3G=['3G_CELLS_IR(Number)','3G Cell_Avail_Sys_IR(%)','3G_RAB_Drop_Rate_CS_IR(%)',
       '3G_RAB_Drop_Rate_HS_IR(%)','3G_HS Frame Loss Ratio_IR(%)','3G_CSSR_CS_IR(%)', '3G_CSSR_PS_IR(%)']
KPIs_4G=['4G_CELLS_IR(#)','4G_CELL_AVAIL_SYS_IR','4G_ERAB_DROP_RATE_IR(%)','4G_CSSR_PS_IR(%)']
KPIs_TDD=['TDD_4G Cell Number(#)']

technology=['2G','3G','4G','TDD']


cell_nember_kpi_dict={'2G_CELLS_IR(#)':'2G','3G_CELLS_IR(Number)':'3G','4G_CELLS_IR(#)':'4G','TDD_4G Cell Number(#)':'TDD'}
cell_nember_kpi_lists=['2G_CELLS_IR(#)','3G_CELLS_IR(Number)','4G_CELLS_IR(#)','TDD_4G Cell Number(#)']
        

#%% adding hour and weekday in target and hourly files


target_province['Time']=pd.to_datetime(target_province['Time'])

target_province['Hour']=target_province['Time'].dt.hour
target_province['week_day']=target_province['Time'].dt.weekday
target_province['week_day'][target_province['week_day'].isin([3 , 4])]=3 # Monday is 0
target_province['week_day'][target_province['week_day']!=3]=1


hourly_province['Time']=pd.to_datetime(hourly_province['Time'])
hourly_province['Hour']=hourly_province['Time'].dt.hour
hourly_province['week_day']=hourly_province['Time'].dt.weekday
hourly_province['week_day'][hourly_province['week_day'].isin([3 , 4])]=3 # Monday is 0
hourly_province['week_day'][hourly_province['week_day']!=3]=1

hourly_province_graph['Time']=pd.to_datetime(hourly_province_graph['Time'])
hourly_province_graph['Hour']=hourly_province_graph['Time'].dt.hour
hourly_province_graph['week_day']=hourly_province_graph['Time'].dt.weekday
hourly_province_graph['week_day'][hourly_province_graph['week_day'].isin([3 , 4])]=3 # Monday is 0
hourly_province_graph['week_day'][hourly_province_graph['week_day']!=3]=1

target_province.to_csv('target_time.csv')
#%%reorganize stats
target_province_copy=copy.copy(target_province)  #target_province=target_province_copy

for cells in cell_nember_kpi_lists:

    del target_province[cells]

#target_province
target_province.dtypes
tuples = list( zip( *[   target_province['Time'],target_province['Hour'],
                      target_province['week_day'],target_province['Province'],target_province['Region']]  ) )
    
target_province.index=pd.MultiIndex.from_tuples(tuples, names=['Time','Hour', 'week_day', 'Province', 'Region'])
del target_province['Time'],target_province['Hour'],target_province['week_day'],target_province['Province'],target_province['Region']

target_province= target_province.stack().reset_index()
target_province=target_province.rename(columns={'level_5':'KPI',0:'Value'})

#target_province=target_province.dropna(subset='Value')
#target_province=target_province[target_province['Time']<]
del target_province['Time']
'''['Hour', 'week_day', 'Province', 'Region', 'KPI', 'Value']'''


#target_percentage
tuples = list( zip( *[target_percentage['Province']]  ) )
    
target_percentage.index=pd.MultiIndex.from_tuples(tuples, names=['Province'])
del target_percentage['Province']

target_percentage= target_percentage.stack().reset_index()
target_percentage=target_percentage.rename(columns={'level_1':'KPI',0:'Percentage'})



#target_fixed
tuples = list( zip( *[target_fixed['Province']]  ) )
    
target_fixed.index=pd.MultiIndex.from_tuples(tuples, names=['Province'])
del target_fixed['Province']

target_fixed= target_fixed.stack().reset_index()

target_fixed=target_fixed.rename(columns={'level_1':'KPI',0:'Target_fixed'})


#hourly_province
tuples = list( zip( *[   hourly_province['Time'],hourly_province['Hour'],
                      hourly_province['week_day'],hourly_province['Province'],hourly_province['Region']]  ) )
    
hourly_province.index=pd.MultiIndex.from_tuples(tuples, names=['Time','Hour', 'week_day', 'Province', 'Region'])
del hourly_province['Time'],hourly_province['Hour'],hourly_province['week_day'],hourly_province['Province'],hourly_province['Region']

hourly_province= hourly_province.stack().reset_index()
hourly_province=hourly_province.rename(columns={'level_5':'KPI',0:'Value'})



#cells target
tuples = list( zip( *[cells_target['Province']]  ) )
    
cells_target.index=pd.MultiIndex.from_tuples(tuples, names=['Province'])
del cells_target['Province']

cells_target= cells_target.stack().reset_index()

cells_target=cells_target.rename(columns={'level_1':'KPI',0:'Cells_Target'})

#%%preparing the target per province and hour and weekday--------------------------------------------------------
#target_province_grouped_std=target_province.groupby(['Hour','week_day','Province','Region','KPI']).std()


if target_province['Value'].dtype=='O': # This part has been added to ignore the cells with negative throughput
        pattern=r'-.*'
        list_thr= target_province['Value']
        for i in list_thr:
            result=re.search(pattern,str(i))
            if result:
                print(i,result.group())
                target_province =target_province[target_province['Value']!=result.group()]
                continue
target_province['Value'] = target_province['Value'].astype(float)  

if hourly_province['Value'].dtype=='O': # This part has been added to ignore the cells with negative throughput
        pattern=r'-.*'
        list_thr= hourly_province['Value']
        for i in list_thr:
            result=re.search(pattern,str(i))
            if result:
                print(i,result.group())
                hourly_province =hourly_province[hourly_province['Value']!=result.group()]
                continue
hourly_province['Value'] = hourly_province['Value'].astype(float)  


target_province_grouped_median=target_province.groupby(['Hour','week_day','Province','Region','KPI']).median()
target_province_grouped_median=target_province_grouped_median.reset_index()
target_province_grouped_median=target_province_grouped_median.rename(columns={'Value':'median'})
# a=target_province_grouped_median[(target_province_grouped_median['Province']=='GILAN') & (target_province_grouped_median['KPI']=='2G_TCH_AVAILABILITY_IR(%)')]   # & (target_province_merged['Hour']==0) & (target_province_merged['week_day']==0)  ]
# a.to_csv('a.csv')

#target_province_merged=pd.merge(target_province,target_province_grouped_median, on=['Hour','week_day','Province','Region','KPI'], how='left', suffixes=('','_median'))
target_province_merged=pd.merge(target_province_grouped_median,target_percentage, on=['Province','KPI'], how='left', suffixes=('','_percentage'))
target_province_merged=pd.merge(target_province_merged,target_fixed, on=['Province','KPI'], how='left', suffixes=('','_fixed'))

target_percentage.dtypes
target_percentage['Percentage'].unique()

target_province_merged['Target_flex']=target_province_merged['median']-(target_province_merged['median']*target_province_merged['Percentage'])
target_province_merged['Target_flex'][target_province_merged['KPI'].isin(decreasing_KPIs)]=target_province_merged['median']+(target_province_merged['median']*target_province_merged['Percentage'])
target_province_merged['Target_flex'][(target_province_merged['KPI'].isin(decreasing_KPIs) ) & (target_province_merged['Target_flex']<0.1)]=0.1



#apply fixed targets------------------------
fixed_KPIs=[]
#Activate this if you want to have fixed targets for any KPI
# fixed_KPIs=['2G_TCH_AVAILABILITY_IR(%)', '3G Cell_Avail_Sys_IR(%)',
#        '4G_CELL_AVAIL_SYS_IR', '2G_CSSR_IR(%)', '3G_CSSR_CS_IR(%)',
#        '3G_CSSR_PS_IR(%)', '4G_CSSR_PS_IR(%)', '2G_DCR_IR(%)',
#        '4G_ERAB_DROP_RATE_IR(%)', '3G_RAB_Drop_Rate_CS_IR(%)',
#        '3G_RAB_Drop_Rate_HS_IR(%)']


#target_province_grouped_fixed=pd.merge(target_province_grouped,target_fixed, on='Province',how='left', suffixes=('','_fixed'))

target_province_merged['Target']=target_province_merged['Target_flex']
target_province_merged['Target'][target_province_merged['KPI'].isin(fixed_KPIs)]=target_province_merged['Target_fixed']


# for kpi in fixed_KPIs:
#     target_province_grouped_fixed[kpi]=target_province_grouped_fixed['{0}_fixed'.format(kpi)]
target_province_grouped=target_province_merged[['Hour', 'week_day', 'Province', 'Region', 'KPI', 'Target']]
#end---------------------------------
target_province_grouped.to_csv('modified_target.csv')

#%% checking with cells target which is omitted now
'''['Time', 'Hour', 'week_day', 'Province', 'Region', 'KPI', 'Value']'''

#----checking last hour stats

hourly_province_cells=hourly_province[hourly_province['KPI'].isin(cell_nember_kpi_lists)][['Time','Province','KPI','Value']]
hourly_province_cells=pd.merge(hourly_province_cells,cells_target, on=['Province','KPI'] , how='left')
#hourly_province_cells['rank']=hourly_province_cells.groupby(['Province','KPI'])["Time"].rank("dense", ascending=False)

hourly_province_cells=hourly_province_cells[hourly_province_cells['Value']>hourly_province_cells['Cells_Target']]

#a=hourly_province_cells[hourly_province_cells['Province']=='ILAM']
hourly_province_cells_grouped=hourly_province_cells.groupby(['Province','KPI'])['Time'].max()
hourly_province_cells_grouped=hourly_province_cells_grouped.reset_index()


hourly_province_cells_grouped['Tech']='2G'
hourly_province_cells_grouped['Tech'][hourly_province_cells_grouped['KPI'].isin(KPIs_3G)]='3G'
hourly_province_cells_grouped['Tech'][hourly_province_cells_grouped['KPI'].isin(KPIs_4G)]='4G'
hourly_province_cells_grouped['Tech'][hourly_province_cells_grouped['KPI'].isin(KPIs_TDD)]='TDD'

#hourly_province_cells['concat']=hourly_province_cells['Province']+hourly_province_cells['KPI']



hourly_province['Tech']='2G'
hourly_province['Tech'][hourly_province['KPI'].isin(KPIs_3G)]='3G'
hourly_province['Tech'][hourly_province['KPI'].isin(KPIs_4G)]='4G'
hourly_province['Tech'][hourly_province['KPI'].isin(KPIs_TDD)]='TDD'

hourly_province=pd.merge(hourly_province,hourly_province_cells_grouped[['Province', 'Time', 'Tech']],on=['Province','Tech'], how='left', suffixes=('','_max'))
#a=hourly_province_merged[hourly_province_merged['Province']=='YAZD']
hourly_province=hourly_province[hourly_province['Time']<=hourly_province['Time_max']]
del hourly_province['Time_max']
#------

hourly_province_time_count=hourly_province.groupby(['Province','KPI'])['Time'].count()
hourly_province_time_count=hourly_province_time_count.reset_index()

hourly_province_merged=pd.merge(hourly_province,target_province_grouped, on=['Hour', 'week_day', 'Province', 'Region', 'KPI'], how='left')
hourly_province_merged=hourly_province_merged[~hourly_province_merged['KPI'].isin(cell_nember_kpi_lists)]



hourly_province_merged.to_csv('check.csv')
hourly_province_merged_copy=copy.copy(hourly_province_merged)
#hourly_province_merged=hourly_province_merged[hourly_province_merged['Value_Cells_numbers']<hourly_province_merged['Cells_Target']]
#%%finding abnormalities
'''['Time', 'Hour', 'week_day', 'Province', 'Region', 'KPI', 'Value',
       'tech', 'Target']'''

hourly_province_merged['rank']=hourly_province_merged.groupby(['Province','KPI'])["Time"].rank("dense", ascending=False)
hourly_province_merged['compare']=hourly_province_merged['Value']-hourly_province_merged['Target'] #compare>0 is ok
hourly_province_merged['compare'][hourly_province_merged['KPI'].isin(decreasing_KPIs)]=-hourly_province_merged['compare']

target_last_hour=hourly_province_merged[hourly_province_merged['rank']==1]
del target_last_hour['rank']

total_hours_problematic=hourly_province_merged[hourly_province_merged['compare']<0]

last_hour_problematic=hourly_province_merged[(hourly_province_merged['compare']<0) & (hourly_province_merged['rank']==1)]
last_hour_problematic['concat']=last_hour_problematic['Province']+last_hour_problematic['KPI']
total_hours_problematic['concat']=total_hours_problematic['Province']+total_hours_problematic['KPI']

last_hour_problematic['Age']=1  


r=1    
for con in last_hour_problematic['concat'] : #con=last_hour_problematic['concat'][0]
    #print(con)
    r=r+1
    print(r)
    temp_df=total_hours_problematic[total_hours_problematic['concat']==con]
    cnt=2
    while cnt in list(temp_df['rank']):
        last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age'][last_hour_problematic['concat']==con]+1
        cnt=cnt+1                        
r=1     
for con in last_hour_problematic['concat']:
   r=r+1
   print(r)
   fluct=1
   while(fluct==1 ):     
                 
       n=last_hour_problematic['Age'][last_hour_problematic['concat']==con].values[0]
       fluct=0
       temp_df=total_hours_problematic[total_hours_problematic['concat']==con]

       if (n+2 in list(temp_df['rank'])) & (n<len(desired_hours_list)-1) : #we need at least 3 hours recoverying to close the aging
           last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age'][last_hour_problematic['concat']==con]+2

           fluct=1

       elif (n+3 in list(temp_df['rank'])) & (n<len(desired_hours_list)-2) :
           last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age'][last_hour_problematic['concat']==con]+3

           fluct=1

       if fluct==1:
           cnt=last_hour_problematic['Age'][last_hour_problematic['concat']==con].values[0]+1
           while cnt in list(temp_df['rank']):
               last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age']+1
               cnt=cnt+1
                
'''['Time', 'Hour', 'week_day', 'Province', 'Region', 'KPI', 'Value',
       'tech', 'Target', 'rank', 'compare', 'Age', 'concat']'''

Output=last_hour_problematic[['Time','Region' ,'Province','Tech', 'KPI', 'Value','Age','Target']]   


#%%RootCause
Output['Cause']=''
RootCause.columns
Output.columns
for province in Output['Province'].unique():
    #print(province)
    df_temp=Output[Output['Province']==province]
    TX=0
    
    for kpi in df_temp['KPI']:
        print(kpi)
        Age_kpi=list(Output['Age'][(Output['Province']==province) & (Output['KPI']==kpi ) ])[0]

        if list(RootCause[RootCause['KPI']==kpi]['TX'])[0] in list(df_temp['KPI']):
            Age_TX= list(Output['Age'][(Output['Province']==province) & (Output['KPI']==list(RootCause[RootCause['KPI']==kpi]['TX'])[0] ) ])[0]
            if Age_TX==Age_kpi:
                Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = ' TX'
            else:
                Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = ' Maybe TX'
            TX=1
            
        if list(RootCause[RootCause['KPI']==kpi]['MS'])[0] in list(df_temp['KPI']):
            Age_MS= list(Output['Age'][(Output['Province']==province) & (Output['KPI']==list(RootCause[RootCause['KPI']==kpi]['MS'])[0] ) ])[0]

            if Age_MS==Age_kpi:
                
                Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] =  Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ]+' MS'
            else:
                Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ]+' Maybe MS'
        
        if len(list(Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi )])[0])==0: 
                 
            if (list(RootCause[RootCause['KPI']==kpi]['KPI1']))[0] in list(df_temp['KPI']) :
                Age_KPI1=list(Output['Age'][(Output['Province']==province) & (Output['KPI']==list(RootCause[RootCause['KPI']==kpi]['KPI1'])[0] ) ])[0]
                if Age_KPI1==Age_kpi:
                    Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] + (list(RootCause[RootCause['KPI']==kpi]['KPI1']))[0]
                else:
                    Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] +'Maybe '+ (list(RootCause[RootCause['KPI']==kpi]['KPI1']))[0]
                      
            if (list(RootCause[RootCause['KPI']==kpi]['KPI2']))[0] in list(df_temp['KPI']):
                Age_KPI2=list(Output['Age'][(Output['Province']==province) & (Output['KPI']==list(RootCause[RootCause['KPI']==kpi]['KPI2'])[0] ) ])[0]
                if Age_KPI2==Age_kpi:
                    Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] +' '+ (list(RootCause[RootCause['KPI']==kpi]['KPI2']))[0]
                else:
                    Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] +' Maybe '+ (list(RootCause[RootCause['KPI']==kpi]['KPI2']))[0]
            
            if (list(RootCause[RootCause['KPI']==kpi]['KPI3']))[0] in list(df_temp['KPI']):
                Age_KPI3=list(Output['Age'][(Output['Province']==province) & (Output['KPI']==list(RootCause[RootCause['KPI']==kpi]['KPI3'])[0] ) ])[0]
                if Age_KPI3==Age_kpi:
                    Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] +' '+ (list(RootCause[RootCause['KPI']==kpi]['KPI3']))[0]
                else:
                    Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] = Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ] +' Maybe '+ (list(RootCause[RootCause['KPI']==kpi]['KPI3']))[0]

            if len(list(Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi )])[0])==0:
                Output['Cause'][(Output['Province']==province) & (Output['KPI']==kpi ) ]=' ' + list(RootCause[RootCause['KPI']==kpi]['Cause'])[0]

Output=pd.merge(Output,hourly_province_time_count,on=['Province','KPI'], how='left',suffixes=('','_maxage'))
Output['temp']='At least '
Output['Age'][Output['Age']==Output['Time_maxage']]=Output['temp'] + Output['Age'].astype(str)
del Output['Time_maxage'],Output['temp']
#Output['Age'][Output['Age']== len(desired_hours_list)]='At least ' + str(len(desired_hours_list))
Output['indexNumber'] = Output['Region'].str[1:].astype(int)
Output=Output.sort_values(by=['indexNumber','Province','Tech', 'KPI']).drop('indexNumber', axis=1)
Output['Target']=np.round(Output['Target'],2)
# try:
#     Output['Value']=np.round(Output['Value'],2)


#%% result

#Output=Output[Output['KPI']!='4G_HOSR_Inter_Freq_Out_IR(%)']  
os.chdir(main_directory_Output)

t=datetime.datetime.now().strftime("%Y%m%d-%H")
with pd.ExcelWriter("Anomaly_{0}.xlsx".format(t), engine='xlsxwriter') as writer:  
    
    Output.to_excel(writer, sheet_name='KPI_issues', index=False)
    hourly_province.to_excel(writer, sheet_name='raw_data', index=False)

    worksheet = writer.sheets['KPI_issues']  # pull worksheet object
    worksheet.active
    datetime.date
    #worksheet.set_column('A:A',23)  # set column width
    for i, col in enumerate(Output.columns):
        #print(i,col)
        
        column_len = Output[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 3
        # set the column length
        worksheet.set_column(i, i, column_len)
        
    hourly_province_cells_grouped[['Province', 'Tech', 'Time']].to_excel(writer, sheet_name='last existing time', index=False)








