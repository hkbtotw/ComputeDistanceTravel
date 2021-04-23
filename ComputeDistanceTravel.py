import pandas as pd
from datetime import datetime, date,  timedelta
import numpy as np
from Database_Interaction import *
import warnings

warnings.filterwarnings("ignore")


start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))


dateFrom = '2021-04-15'
dateTo = todayStr  #'2021-04-08'

df_origin=ReadOriginData()
employeeList=list(df_origin['Employee_ID'].unique())

#employeeList=['15567828','15567645','70050222','41123012','11029584','11005973']


count=0
idWithNoDataFlag=0
idWithDataFlag=0

mainDf=pd.DataFrame(columns=['Employee_ID','Date','distanceTravel', 'DBCreatedDateTime'])
totalDf=pd.DataFrame(columns=['Employee_ID','NumberDays','TotalTravel','DBCreatedDateTime'])
for eId in employeeList[:20]:
    count+=1
    print(' === > ',eId, ' ::  ',count)
    #eId=employeeList[1]

    df_input=ReadTransactionData(eId, dateFrom, dateTo)
    if(len(df_input)>0):
        df_input=df_input.sort_values(by=['DateTimeStamp'], ascending=True).reset_index(drop=True)

        # convert column numpy datetime64 to datetime
        df_input['mapped_DateTimeStamp']=df_input['DateTimeStamp'].dt.to_pydatetime()
        #print(len(df_input), ' ------  ',df_input)

        dateStart, dateEnd=GetStartEndDate(df_input)
        dfDummy=TransformIndividualData(df_input, eId, dateStart, dateEnd)
        dfDummy['DBCreatedDateTime']=nowStr
        #print(dfDummy)

        NumberDays=len(dfDummy)
        TotalTravel=dfDummy['distanceTravel'].sum(axis=0, skipna=True)


        totalDf=totalDf.append({'Employee_ID': eId, 'NumberDays': NumberDays, 'TotalTravel': TotalTravel, 'DBCreatedDateTime':nowStr}, ignore_index=True) 

        mainDf=mainDf.append(dfDummy)
        idWithDataFlag+=1
    else:
        idWithNoDataFlag+=1

mainDf['Date']=mainDf['Date'].astype(str)
print(mainDf)
print(totalDf)
print(' WithData ', idWithDataFlag, ' ----  NoData ', idWithNoDataFlag)

Write_DistanceEmployee_to_database(mainDf)

Write_DistanceTotal_to_database(totalDf)

## Write log file
file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\DistancingScoreVsCOVIDStatus\\'
activityLog=' DistanceTravel Successful at '+nowStr+ ' ******** \n'

log_file="DistanceTravel_"+todayStr
f = open(file_path+'\\log\\'+log_file, "a")
f.write(activityLog)
f.close()


del df_input, df_origin, mainDf, totalDf

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')