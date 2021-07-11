# -*- coding: utf-8 -*-
"""
Created on Tue Jul  6 12:54:20 2021

@author: lopezf
"""
import pyodbc
import pandas as pd
import os
import numpy as np
from sqlalchemy import create_engine
import urllib
import datetime as dt
from dateutil.relativedelta import relativedelta
import time
from pathlib import Path
import gc

paths = {
            "med": "I:/COF/COF/_M3trics2/records/med_parquet"
            ,"shl": "I:/COF/COF/_M3trics2/records/shl_parquet"
    }


#FUNCTIONS----------------------------------------------------------

def pull_range(dir,start, stop):
     start_date_time = dt.datetime.strptime(start, '%Y-%m-%d')
     stop_date_time = dt.datetime.strptime(stop, '%Y-%m-%d')
     
     return [file
            for file in Path(dir).glob('*')
            if (start_date_time <= 
                dt.datetime.strptime(file.stem.split('_')[-1], '%Y-%m-%d')
                <= stop_date_time)
            ]

def pull_month(mnth, taxi_type):
    
    #define columns
    cols = ["hack",taxi_type,"pudt","dodt","fare","surcharge"
           ,"mtaTax","tip","tolls","improveSurch","distance"
           ,"ehailFee","trip_time_hours","total_amount"]
    
    #produce start and end
    start = mnth
    stop = str(dt.datetime.strptime(mnth, '%Y-%m-%d') + relativedelta(months=+1) + dt.timedelta(days=-1))[0:10]
    print(start)
    print(stop)
    
    #generate sequence of dates and extract directory files
    my_days = pd.date_range(start=start,end=stop)
    filez = pull_range(paths[taxi_type], start, stop)
    
    #loop through the files, read and bind them
    trips=[]
    for file in filez:
        df = pd.read_parquet(file, engine='pyarrow')
        
        #alter trip time into hours for shift calculations
        df['trip_time_hours'] = df['trip_time_secs']/3600

        
        trips.append(df)
        del(df)
        gc.collect()
    
    #bind to one data frame
    trips= pd.concat(trips, axis=0, ignore_index=True)
    
    #keep trips that are shorter than or equal to 4 hours as these are considered valid
    trips = trips[trips["trip_time_hours"] <= 4]
    
    #clean any leakage in trips that might have been coded wrong by DBA
    trips = trips.loc[trips.pudt >= start]
    trips = trips.loc[trips.pudt <= dt.datetime.strptime(mnth, '%Y-%m-%d') + relativedelta(months=+1)]

    #keep only the columns I need
    trips = trips[cols]
    del(cols, filez, my_days)
    gc.collect()
    
    return trips

trips = pull_month('2019-01-01',"shl")


def calculate_shift(df, taxi_type, rest):
    
    start_time = time.time()
    print('starting shift calculation...')
    
    group_by = ['hack'
                ,taxi_type
                ,'pudt']
    
    #set order of data set by driver and vehicle
    df = df.sort_values(by=group_by)
    
    #calculate cruise time between trips
    df['break_from_last'] = df['dodt'] - df.groupby(['hack'
                                                     , taxi_type
                                                     ])['pudt'].shift()
    
    #change calculation to hours
    df['break_from_last'] = df['break_from_last']/np.timedelta64(1, 'h')
    
    #calculate shift break
    df.loc[df['break_from_last'] >= rest, 'shift_status'] = 'shift_start'
    
    #any break from last that are empty are single trip shifts
    df.loc[(pd.isnull(df.break_from_last)), 'shift_status'] = 'shift_start'

    #generate a shift id and expand
    #anytime there is a shift start, generate a new id and then fill the value forward
    df['shift_id']=np.where(df['shift_status'].isna(),np.nan,df['shift_status'].eq('shift_start').cumsum())
    
    #fill shift_id forward
    df['shift_id'] = df['shift_id'].fillna(method="ffill")
    
    te = str(time.time() - start_time)
    print('shift calculation completed in ' + te + ' seconds.')
    
    return(df)


def time_calculations(df, group_by):

    
     #total hours per shift   
     temp = (
         df.groupby(group_by)['dodt'].last() - df.groupby(group_by)['pudt'].first()
         )/np.timedelta64(1, 'h')
     
     temp = temp.to_frame(name='total_hours_per_shift')
     df = pd.merge(df,temp, on=group_by, how='left')
     df['total_hours_per_shift'] = df['total_hours_per_shift'].round(2)
     del(temp)
     
     #calculate cruise hours per shift
     df['cruise_hours_per_shift'] = (df['total_hours_per_shift'] - df['trip_hours_per_shift']).round(2)
     
     #retrieve last dropoff as the end of shift 
     df['shift_end_time'] = df.groupby(group_by)['dodt'].transform('last')
     
     #rename pudt as shift start time - we will remove dups and keep the first row to preserve first and last trip for each shift
     df.rename(columns={'pudt': 'shift_start_time'}, inplace=True)
     
     return df

def metrics_builder(df, taxi_type):
    
    start_time = time.time()
    print('starting metrics build...')
    
    #grouping list
    group_by = ['hack'
                ,taxi_type
                ,'shift_id']
    
    #counts
    df['trips_per_shift'] = df.groupby(group_by)['pudt'].transform("count")    
     
    #summations
    df['trip_hours_per_shift'] = (df.groupby(group_by)['trip_time_hours'].transform("sum")).round(2)
    df['distance_per_shift'] = df.groupby(group_by)['distance'].transform('sum')
    df['fare_per_shift'] = df.groupby(group_by)['fare'].transform('sum')
    df['tip_per_shift'] = df.groupby(group_by)['tip'].transform('sum')
    df['surcharge_per_shift'] = df.groupby(group_by)['improveSurch'].transform('sum')
    df['total_amount_per_shift'] = df.groupby(group_by)['total_amount'].transform('sum')

    #hours per shift and cruise time per shift
    df = time_calculations(df,group_by)
    
    #final cleanup and calculations
    df = df.drop_duplicates(subset=['shift_id'])    
    
    #code shift type as AM or PM
    df['shift_type'] = np.where(df['shift_start_time'].dt.hour < 12 , 'AM', 'PM') 
    
    #code the weekday for later 
    df['metric_weekday'] = df['shift_start_time'].dt.day_name()
    
    #code the month and year
    df['metric_month'] = df['shift_start_time'].dt.date.astype(str).str.slice(start=0, stop=7) + '-01'
    
    #reorder columns for final result
    cols = ["hack",taxi_type,"shift_id","shift_start_time"
            ,"shift_end_time","trips_per_shift","total_hours_per_shift"
            ,"trip_hours_per_shift","cruise_hours_per_shift","distance_per_shift"
            ,"fare_per_shift", "tip_per_shift","surcharge_per_shift"
            ,"total_amount_per_shift","shift_type","metric_weekday", "metric_month"]
    print(df['metric_month'].unique())
    df = df[cols]
    
    te = str(time.time() - start_time)
    print('metrics build completed in ' + te + ' seconds.')
    
    
    return df


#load to sql 
def load_to_sql(df, taxi_type, con):
    #load existing table to a temp structure in SQL
    #no need to create the table, let pandas handle that
    start_time = time.time()
    print('Loading to Database...')
    df.to_sql(name = taxi_type + '_monthly_shift_metrics_temp',con = con, index = False)
    te = str(time.time() - start_time)
    print('Successfully Loaded in ' + te + ' seconds.' )
    
    #insert records into main table
    #write sql syntax to insert records into main table
    insert_sql = '''insert into ''' + taxi_type + '''_monthly_shift_metrics (
            hack,''' + taxi_type + ''' 
            ,shift_id,
            shift_start_time,
            shift_end_time,
            trips_per_shift,
            total_hours_per_shift,
            trip_hours_per_shift,
            cruise_hours_per_shift,
            distance_per_shift,
            fare_per_shift,
            tip_per_shift,
            surcharge_per_shift,
            total_amount_per_shift,
            shift_type,
            metric_weekday,
            metric_month
            )
            select * from ''' + taxi_type + '''_monthly_shift_metrics_temp;'''
            
    drop_sql = '''DROP TABLE IF EXISTS ''' + taxi_type + '''_monthly_shift_metrics_temp;'''

    #basic row count before main table is updated
    prev_count = '''select count(*) as records_before_update from ''' + taxi_type + '''_monthly_shift_metrics;'''
    #row count after main table is updated
    post_count = '''select count(*) as records_after_update from ''' + taxi_type + '''_monthly_shift_metrics;'''
    
    
    con.execute(insert_sql)
    con.execute(drop_sql)
    prev = con.execute(prev_count).fetchall()
    post = con.execute(post_count).fetchall()
    print(prev)
    print(post)
    

#run the pipeline
def run_shifts(mnth, taxi_type, rest, con):
    #collect all the trips for the month 
    
    trips = pull_month(mnth, taxi_type)
    trips = calculate_shift(trips, taxi_type, rest)
    trips = metrics_builder(trips, taxi_type)
    load_to_sql(trips, taxi_type ,con)
    del(trips)
    gc.collect()
   
    



    
   
trips = pull_month('2019-01-01',"shl")
trips = calculate_shift(trips, "shl", 3)
trips = metrics_builder(trips,'shl')
 
#params
server= 'TLCBDBDEV1'
driver= '{ODBC Driver 17 for SQL Server}'
database='policy_programs'
username= 'lopezf@tlc.nyc.gov'

#initiate connection
params = urllib.parse.quote_plus("Driver=" + driver +";Server=" + server +";DATABASE=" + database + ";Trusted_Connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

run_shifts('2019-02-01','shl', 3, engine)

#	hack	shl
#312472	001102	AA132

mnths = ['2019-01-01', '2019-02-01']
for i in mnths:
    run_shifts(i,'shl', 3, engine)


#z = trips.loc[trips.hack == '001102']
