# -*- coding: utf-8 -*-
"""
Created on Tue Jul  6 12:54:20 2021

@author: lopezf
"""
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

from pathlib import Path


    
   
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
