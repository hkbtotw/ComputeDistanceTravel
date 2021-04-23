from datetime import datetime, date,  timedelta
import pyodbc
import skmob
from skmob.measures.individual import distance_straight_line
import numpy as np
import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def ReadTransactionData(eId, dateFrom, dateTo):
    #print('------------- Start ReadDB -------------')
    dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    ## ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCTSREMP;'
                            'Database=SR_APP;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

    /****** Script for SelectTopNRows command from SSMS  ******/
    
    declare @dateFrom date = '"""+str(dateFrom)+"""'
    declare @dateTo date = '"""+str(dateTo)+"""'
    declare @eId nvarchar(15) = '"""+str(eId)+"""'
        

    SELECT  [employee_id] as [EmployeeId]
        ,[latitude]  as [UserLat]
        ,[longitude] as [UserLong]
        ,[create_date] as [DateTimeStamp]
    FROM [SR_APP].[dbo].[TB_SR_Covid_location]
    where cast([create_date] as date)>=@dateFrom
        AND  cast([create_date] as date)<= @dateTo
        AND [employee_id]=@eId 
    UNION
    SELECT  
        [EmployeeId]      
        --,[UserLat]
        --,[UserLong]
        ,case when [UserLat]=0 then [LocationLat] else [UserLat] end [UserLat]
        ,case when [UserLong]= 0 then [LocationLong] else [UserLong] end [UserLong]
        --,[LocationLat] as [UserLat]
        --,[LocationLong] as [UserLong]
        ,[CreatedDateTime]      
    FROM [SR_APP].[dbo].[TB_QR_TimeStamp]
    where cast([CreatedDateTime] as date)>=@dateFrom
        AND  cast([CreatedDateTime] as date)<= @dateTo
        AND [EmployeeId]=@eId
    UNION
    SELECT 
        [EmployeeId]
        ,case when [UserLat]=0 then [LocationLat] else [UserLat] end [UserLat]
        ,case when [UserLong]= 0 then [LocationLong] else [UserLong] end [UserLong]
        ,[CreatedDateTime]      
    FROM [SR_APP].[dbo].[TB_Checkin_PG]
    where cast([CreatedDateTime] as date)>=@dateFrom
        AND  cast([CreatedDateTime] as date)<= @dateTo
        AND [EmployeeId]=@eId 

    """
    dfout=pd.read_sql(sql,conn)
    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)
    dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql

    return dfout

def ReadLocationData():
    print('------------- Start ReadDB -------------')
    dfout = pd.DataFrame(columns=['COVID_EMPID','COVID_STATUS','COVID_STATUS_LABEL','answer_id','CREATED_Date','CREATED_Datetime','row_num','DONE_ON'])
    ## ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCTSREMP;'
                            'Database=SR_APP;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

    SET ansi_warnings OFF

    declare @fromdate date = cast(dateadd(DAY,-15,getdate()) as date)
    declare @today date = cast(getdate() as date)
    declare @todate date = @today

    /* Main */
    select AA.* 
    from (

        /* Table B */
        select COVID_EMPID
            ,COVID_STATUS 
            ,case when COVID_STATUS in ('0') then N'0.ยังระบุไม่ได้'
                when COVID_STATUS in ('1','D') then N'D.ปกติ' 
                when COVID_STATUS in ('2','C') then N'C.ความเสี่ยงต่ำ' 
                when COVID_STATUS in ('3','B') then N'B.ความเสี่ยงปานกลาง' 
                when COVID_STATUS in ('5','B1') then N'B1.ความเสี่ยงสูง' 
                when COVID_STATUS in ('4','A') then N'A.ความเสี่ยงสูงมาก' 
                else '' end as COVID_STATUS_LABEL
            ,answer_id 
            ,COVID_Date CREATED_Date
            ,COVID_Datetime CREATED_Datetime
            ,ROW_NUMBER() OVER ( PARTITION BY COVID_EMPID,COVID_Date ORDER BY COVID_Datetime desc ) row_num
            ,DONE_ON

        from (
            --------------------------------------------------------------------------------------------------------------------------------------------
            /* COVID_WEB  */
            select COVID_EMPID, COVID_STATUS, COVID_STATUS_GRP, COVID_Datetime, COVID_Date, DONE_ON ,answer_id
                ,q1,q5_province_id,q5_province_nm,q6_province_id,q6_province_nm,risk_area_status
                ,'answer_web' as [sys_ver]
            from ( select [employee_id] COVID_EMPID
                ,[covid_code] as COVID_STATUS ,[covid_code] as COVID_STATUS_GRP
                ,cast(convert(datetime, [created_date]) as datetime) COVID_Datetime, cast(convert(datetime, [created_date]) as date) COVID_Date
                ,W.id as answer_id,  q1 
                ,case when q1_check_covid_date = '' or q1_check_covid_date is null then NULL else q1_check_covid_date end as q1_check_covid_date
                ,q5_province_id, q5_province_name q5_province_nm, q6_province_id, q6_province_name q6_province_nm,risk_area_status
                ,ROW_NUMBER() OVER ( PARTITION BY [employee_id], cast(convert(datetime, [created_date]) as date) ORDER BY convert(datetime, [created_date]) desc ) row_num
                ,'WEB' DONE_ON
            FROM [SR_APP].[dbo].TB_SR_Covid_answer_web_four W
                left join [SR_APP].[dbo].[TB_SR_Covid_provinces] P5 on q5_province_name = P5.[name_th]
                left join [SR_APP].[dbo].[TB_SR_Covid_provinces] P6 on q6_province_name = P6.[name_th]
            where Remark = 'Employee' and len([employee_id]) = 8 and cast([created_date] as date) >= @fromdate and cast([created_date] as date) <= @todate
            ) AA where AA.row_num = 1

            union
            /* COVID_APP  */
            --INSERT INTO @COVID_ALL
            select COVID_EMPID, COVID_STATUS,COVID_STATUS_GRP,COVID_Datetime, COVID_Date ,'APP' DONE_ON
                ,answer_id,q1,q5_province_id,q5_province_nm,q6_province_id,q6_province_nm,risk_area_status,[sys_ver]
            from (select E.[employee_id] COVID_EMPID ,A.[COVID_STATUS] 
                    ,[COVID_STATUS] as COVID_STATUS_GRP
                    ,A.COVID_Datetime, A.COVID_Date
                    ,answer_id,q1,q5_province_id,q5_province_nm,q6_province_id,q6_province_nm,risk_area_status
                    ,[sys_ver]
                    ,ROW_NUMBER() OVER ( PARTITION BY E.[employee_id],A.COVID_Date ORDER BY A.COVID_Datetime desc ) row_num
                FROM 
                (	select ath.[id] as answer_id
                    ,ath.[employee_iid] as [employee_id]
                    ,ath.[q1]
                    ,ath.q5_province_id ,P5.name_th as q5_province_nm
                    ,ath.q6_province_id ,P6.name_th as q6_province_nm
                    ,ath.[risk_area_status]
                    ,ath.[create_date]
                    ,case ath.[covid_status]
                        when 5 then 'B1'
                        when 4 then 'A'
                        when 3 then 'B'
                        when 2 then 'C'
                        when 1 then 'D'
                        end as [COVID_STATUS]
                    ,cast(ath.[create_date] as date) as COVID_Date
                    ,ath.[create_date] as COVID_Datetime
                    ,'answer_four' as [sys_ver] 
                    from (select * from [SR_APP].[dbo].[TB_SR_Covid_answer_four] where [covid_status] is not null and employee_iid <> '' and cast(create_date as date) >= @fromdate and cast(create_date as date) <= @todate) ath
                    left join [SR_APP].[dbo].[TB_SR_Covid_provinces] P5 on ath.q5_province_id = P5.[id]
                    left join [SR_APP].[dbo].[TB_SR_Covid_provinces] P6 on ath.q6_province_id = P6.[id]
                ) A
                left join [SR_APP].[dbo].[TB_SR_Covid_employee] E on A.[employee_id] = E.[iid]
                where  A.[covid_status] is not null and E.[employee_id] <> '' and len(E.[employee_id]) = 8 and A.COVID_Date >= @fromdate and A.COVID_Date <= @todate
                ) COV_STS
            where COV_STS.row_num = 1
            --------------------------------------------------------------------------------------------------------------------------------------------
        /* End Table B */
        ) B where B.COVID_EMPID is not null 

        /* End Table AA */
        ) AA where AA.row_num = 1 
        order by CREATED_Datetime
    
    """

    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    dfout.columns=['COVID_EMPID','COVID_STATUS','COVID_STATUS_LABEL','answer_id','CREATED_Date','CREATED_Datetime','row_num','DONE_ON']
    del conn, cursor, sql
    print(' --------- Reading End -------------')

    return dfout

def ReadScoreData():
    print('------------- Start ReadDB -------------')
    dfout = pd.DataFrame(columns=['EID','ELat','Elong','Freq','TotalCheckIn','PercentMovement','MeanCheckIn','MovementWeight','DistancingScore','DBCreatedDateTime']
            )
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCTSREMP;'
                            'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

     SELECT [EID]
      ,[ELat]
      ,[Elong]
      ,[Freq]
      ,[TotalCheckIn]
      ,[PercentMovement]
      ,[MeanCheckIn]
      ,[MovementWeight]
      ,[DistancingScore]
      ,[DBCreatedDateTime]
    FROM [TB_SR_Employee].[dbo].[Mobility_DistancingScore]
   

    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    dfout.columns=['EID','ELat','Elong','Freq','TotalCheckIn','PercentMovement','MeanCheckIn','MovementWeight','DistancingScore','DBCreatedDateTime']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def ReadBStatusData():
    print('------------- Start ReadDB -------------')
    dfout = pd.DataFrame(columns=['COVID_EMPID','dateStart','dateEnd','statusString','BCount','dateB','ACount','dateA','DBCreatedDateTime']
            )
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCTSREMP;'
                            'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

     SELECT [COVID_EMPID]
      ,[dateStart]
      ,[dateEnd]
      ,[statusString]
      ,[BCount]
      ,[dateB]
      ,[ACount]
      ,[dateA]
      ,[DBCreatedDateTime]
    FROM [TB_SR_Employee].[dbo].[Mobility_BStatus]
   
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    dfout.columns=['COVID_EMPID','dateStart','dateEnd','statusString','BCount','dateB','ACount','dateA','DBCreatedDateTime']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def ReadOriginData():
    print('------------- Start ReadDB -------------')
    dfout = pd.DataFrame(columns=['Employee_ID','Origin_location','Last_location','date','CrossPrvFlag']
            )   

    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCTSREMP;'
                            'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

     SELECT  [Employee_ID]
      ,[Origin_location]
      ,[Last_location]
      ,[date]
      ,[CrossPrvFlag]
    FROM [TB_SR_Employee].[dbo].[Mobility_Origin]
   
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    dfout.columns=['Employee_ID','Origin_location','Last_location','date','CrossPrvFlag']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

# Remove Null information in date list
def CreateDateList(dateList):
    #print(' ==> ',dateList)
    dummyList=[]
    for n in dateList:
        try:
            dummyList.append(datetime.strptime(n, '%Y-%m-%d'))
        except:
        #    print(' no date ')
            continue
    dateList=dummyList
    del dummyList
    #print(' -d- ', dateList)   
    return dateList 


def GetStartEndDate(df_input):
    dfHead=df_input.head(1)
    dfTail=df_input.tail(1)

    # # convert numpy64 to date
    # dateStart=dfHead['DateTimeStamp'].values[0].astype('M8[D]')    
    # dateEnd=dfTail['DateTimeStamp'].values[0].astype('M8[D]')    

    dateStart=dfHead['mapped_DateTimeStamp'].values[0]
    dateEnd=dfTail['mapped_DateTimeStamp'].values[0]

    # convert numpy64 date to string
    dateStart_str=str(np.datetime_as_string(dateStart, unit='D'))
    dateEnd_str=str(np.datetime_as_string(dateEnd, unit='D'))

    # print(dateStart, ' ----  ',type(dateStart))
    # print(dateEnd, ' ----  ',type(dateEnd))
    # print(dateStart_str, ' ----  ',type(dateStart_str))
    # print(dateEnd_str, ' ----  ',type(dateEnd_str)) 

    return dateStart_str, dateEnd_str

def RemoveZeroLatLon(df_input):
    df_input=df_input[df_input['UserLat']>0.0]
    df_input=df_input[df_input['UserLong']>0.0]
    dfout=df_input
    #print('Result ; ',dfout)    
    return dfout

def GetDistance(df_input):
    df_input=RemoveZeroLatLon(df_input)
    #print(' ==> ',len(df_input))
    if(len(df_input)>0):                
        #print(df_input)
        tdf = skmob.TrajDataFrame(df_input, latitude='UserLat', longitude='UserLong', datetime='DateTimeStamp', user_id='EmployeeId')
        dsl_df = distance_straight_line(tdf)
        #print(dsl_df)
        dfHead=dsl_df.head()
        distanceTravel=dfHead['distance_straight_line'].values[0]
        del dfHead, dsl_df, tdf
    else:
        distanceTravel=0    
    return distanceTravel

def TransformIndividualData(data_In, nId, start_date, end_date):

    dfDummy=data_In[data_In['EmployeeId']==nId]

    dateList=[start_date,end_date]
    dateList=CreateDateList(dateList)

    dummyList=[]
    dummyList_2=[]
    dummyList_3=[]

    for n in range(0,len(dateList)-1):        
        dateRange=[]
        count=0
        for x in range((dateList[n+1]-dateList[n]).days):                    
            count=count+1
            dateRange.append(dateList[n]+timedelta(days=x))
         

        dummyList=dummyList+dateRange
        #print(' dr -',dummyList)


    dummyList.append(dateList[len(dateList)-1])

    dfDummy['mapped_DateTimeStamp_2']=pd.to_datetime(dfDummy['mapped_DateTimeStamp']).apply(lambda x:x.date())

    idList=[]
    distanceList=[]
    for n in dummyList:
        dfDummy_2=dfDummy[dfDummy['mapped_DateTimeStamp_2']==n.date()].copy()
        #print(dfDummy_2)
        if(len(dfDummy_2)>1):
            distanceTravel=GetDistance(dfDummy_2)            
        else:
            distanceTravel=0
        #print(n, ' ---  ',distanceTravel)
        distanceList.append(distanceTravel)
        idList.append(nId)


    Transform_Data = {    
        'Employee_ID': idList,
        'Date':dummyList,
        'distanceTravel':distanceList,
        }

    data_TF = pd.DataFrame(Transform_Data, columns = ['Employee_ID','Date','distanceTravel'])
    
    del dummyList, idList, distanceList
    
    #print(' ==> ',data_TF)
    return data_TF


def Write_DistanceEmployee_to_database(df_input):
    print('------------- Start WriteDB -------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)

    df_input=df_input.replace({np.nan:None})

    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn1 = pyodbc.connect('Driver={SQL Server};'
                        'Server=SBNDCTSREMP;'
                        'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')

    #- View all records from the table
    
    sql="""delete from [TB_SR_Employee].[dbo].[Mobility_DistanceEmployee]"""
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TB_SR_Employee].[dbo].[Mobility_DistanceEmployee](	
        [Employee_ID]
      ,[Date]
      ,[distanceTravel]
      ,[DBCreatedDateTime]
	)     
    values(?,?,?,?
    
    )""", 
    row['Employee_ID']
    ,row['Date']
    ,row['distanceTravel']    
    ,row['DBCreatedDateTime']      
     ) 
    conn1.commit()

    cursor.close()
    conn1.close()
    print('------------Complete WriteDB-------------')

def Write_DistanceTotal_to_database(df_input):
    print('------------- Start WriteDB -------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)

    df_input=df_input.replace({np.nan:None})

    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn1 = pyodbc.connect('Driver={SQL Server};'
                        'Server=SBNDCTSREMP;'
                        'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')

    #- View all records from the table
    
    sql="""delete from [TB_SR_Employee].[dbo].[Mobility_DistanceTotal]"""
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TB_SR_Employee].[dbo].[Mobility_DistanceTotal](	
        [Employee_ID]
      ,[NumberDays]
      ,[TotalTravel]
      ,[DBCreatedDateTime]
	)     
    values(?,?,?,?
    
    )""", 
    row['Employee_ID']
    ,row['NumberDays']
    ,row['TotalTravel']    
    ,row['DBCreatedDateTime']      
     ) 
    conn1.commit()

    cursor.close()
    conn1.close()
    print('------------Complete WriteDB-------------')