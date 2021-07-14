# -*- coding: utf-8 -*-
"""
Created on Tue Jul  6 12:54:20 2021

@author: lopezf
"""

#import all functions
import time
from etl import * 
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import urllib
import gc

#database params
server= 'TLCBDBDEV1'
driver= '{ODBC Driver 17 for SQL Server}'
database='policy_programs'
username= 'lopezf@tlc.nyc.gov'

#initiate connection
params = urllib.parse.quote_plus("Driver=" + driver +";Server=" + server +";DATABASE=" + database + ";Trusted_Connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

#generate sequence of dates and extract directory files
#define data paths
paths ={
        'med':'I:/COF/COF/_M3trics2/records/med_parquet',
        'shl':'I:/COF/COF/_M3trics2/records/shl_parquet',
        'med_cache': 'I:/COF/COF/_M3trics2/records/med_shift_metrics_cache',
        'shl_cache': 'I:/COF/COF/_M3trics2/records/shl_shift_metrics_cache'
    }

#main function
def run_shifts(mnth, taxi_type, rest, con, dirs):
    start_time = time.time()
    print('running shifts for the month of: ' + mnth)
    #collect all the trips for the month 
    trips = pull_month(mnth, taxi_type, dirs)
    #calculate shifts
    trips = calculate_shift(trips, taxi_type, rest)
    #build out metrics
    trips = metrics_builder(trips, taxi_type)
    #cache metrics
    cache_metrics(mnth, trips, taxi_type, dirs)
    #load to db
    load_to_sql(trips, taxi_type ,con)
    #clean up
    del(trips)
    gc.collect()
    te = str(time.time() - start_time)
    print('month shifts processed in ' + te + ' seconds.')
    #return trips

#run function
if __name__ == "__main__":
    #params
    #mnth = '2019-01-01'
    taxi_type = 'med'
    rest = 4
    mnth = str(date.today())
    mnth = str((datetime.strptime(mnth, '%Y-%m-%d') + relativedelta(months=-1)).date())
    
    #public_cols = ["hack",taxi_type,"pudt","dodt","break_from_last","shift_status","shift_id"]
    
    trips = run_shifts(mnth, taxi_type, rest, engine, paths)[0:50000][public_cols]