# -*- coding: utf-8 -*-
"""
Created on Tue Jul  6 12:54:20 2021

@author: lopezf
"""

#import all functions
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

#main function
def run_shifts(mnth, taxi_type, rest, con):
    print('running shifts for the month of: ' + mnth)
    #collect all the trips for the month 
    trips = pull_month(mnth, taxi_type)
    trips = calculate_shift(trips, taxi_type, rest)
    trips = metrics_builder(trips, taxi_type)
    load_to_sql(trips, taxi_type ,con)
    del(trips)
    gc.collect()

#run function
if __name__ == "__main__":
    mnth = str(date.today())
    mnth = str((datetime.strptime(mnth, '%Y-%m-%d') + relativedelta(months=-1)).date())
    run_shifts(mnth, 'shl', 3, engine)