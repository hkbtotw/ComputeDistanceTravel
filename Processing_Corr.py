import pandas as pd
from datetime import datetime, date,  timedelta
from math import radians, cos, sin, asin, sqrt
import numpy as np
import warnings
from Database_Interaction import *

warnings.filterwarnings("ignore")

#--------------------------------------------------------------------------------------

def ConvertStrToDate(x):
    return datetime.strptime(x,'%Y-%m-%d %H:%M:%S').date()

def ConvertDateToStr(x):
    return x.strftime("%Y-%m-%d")

def CheckABC(dummyList):
    #dummyList=['D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D']
    countD=0
    countABC=0
    for n in dummyList:    
        #print(n, ' ----  ',type(n))
        if(n=='A' or n=='B' or n=='C'):
            #countABC+=1
            countABC=1
        else:
            countD+=1
    #print(countABC, ' ::  ',countD)

    del dummyList, countD, n
    return countABC

def ConvertStatusToList(x):
    return x.split(',')

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

#--------------------------------------------------------------------------------------

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))


#df_input=ReadLocationData()
df_score=ReadScoreData()
df_B=ReadBStatusData()
df_origin=ReadOriginData()
#print('status :',len(df_input))
print('score : ',len(df_score))
print('B :',len(df_B))

df_base=df_score[df_score['TotalCheckIn']>0.0].copy().reset_index(drop=True)
print(len(df_base),' ::base: ',df_base.head(10))

df_B['mapped_dateEnd'] = df_B.apply(lambda x: ConvertStrToDate(x['dateEnd']),axis=1)

dfDummy=df_B[df_B['mapped_dateEnd']==date.today()].copy().reset_index(drop=True)
print(len(dfDummy),' : dummy : ',dfDummy.head(10))

dfDummy['statusList']=dfDummy.apply(lambda x: ConvertStatusToList(x['statusString']),axis=1)
dfDummy['checkABC']=dfDummy.apply(lambda x: CheckABC(x['statusList']),axis=1)
#print(dfDummy.head(100))

df_base_list=list(df_base['EID'].unique())
dfDummy_list=list(dfDummy['COVID_EMPID'].unique())
df_origin_list=list(df_origin['Employee_ID'].unique())
print(len(df_base_list),' ======= ',len(dfDummy_list), ' =====  ',len(df_origin_list))

commonIdList=intersection(df_base_list, dfDummy_list)
commonIdList=intersection(commonIdList, df_origin_list)
print(' ===> ',len(commonIdList))

df_base_2=df_base[df_base['EID'].isin(commonIdList)].copy().reset_index(drop=True)
df_base_2['key']=df_base_2['EID']
print(len(df_base_2),' ===> ',df_base_2)
dfDummy_2=dfDummy[dfDummy['COVID_EMPID'].isin(commonIdList)].copy().reset_index(drop=True)
dfDummy_2['key']=dfDummy_2['COVID_EMPID']
print(len(dfDummy_2),' ===> ',dfDummy_2)
df_origin_2=df_origin[df_origin['Employee_ID'].isin(commonIdList)].copy().reset_index(drop=True)
df_origin_2['key']=df_origin_2['Employee_ID']

mergeDf=pd.merge(df_base_2, dfDummy_2, on=['key']).reset_index(drop=True)
mergeDf=pd.merge(mergeDf,df_origin_2, on=['key']).reset_index(drop=True)

mergeDf=mergeDf[['EID','ELat','Elong','Freq','TotalCheckIn','PercentMovement','DistancingScore','checkABC','BCount','ACount','CrossPrvFlag']]

#df1=mergeDf[['DistancingScore','checkABC']].copy()
#df1=mergeDf[['PercentMovement','checkABC']].copy()
#df1=mergeDf[['DistancingScore','BCount']].copy()
corr=mergeDf.corr()
print(' corr :', corr)






file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\DistancingScoreVsCOVIDStatus\\'
mergeDf.to_csv(file_path+'check.csv')




del dfDummy, df_B, df_score, df_base, mergeDf

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')

