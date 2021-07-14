# -*- coding: utf-8 -*-
"""
Created on Tue Jul  6 12:54:20 2021

@author: lopezf
"""
#this file is meant to offer a parallelized version for multiple month pulls--------------------

#import all functions
import time
from multiprocessing import Pool
from etl import * 
from sqlalchemy import create_engine
import urllib
import gc
from functools import partial

#paths
paths ={
        'med':'I:/COF/COF/_M3trics2/records/med_parquet',
        'shl':'I:/COF/COF/_M3trics2/records/shl_parquet',
        'med_cache': 'I:/COF/COF/_M3trics2/records/med_shift_metrics_cache',
        'shl_cache': 'I:/COF/COF/_M3trics2/records/shl_shift_metrics_cache'
    }

#main function
def run_shifts(mnth, taxi_type, rest, dirs):
    print('running shifts for the month of: ' + mnth)
    #collect all the trips for the month 
    trips = pull_month(mnth, taxi_type, dirs)
    trips = calculate_shift(trips, taxi_type, rest)
    trips = metrics_builder(trips, taxi_type)
    cache_metrics(mnth, trips, taxi_type, dirs)
    #load_to_sql(trips, taxi_type ,con)
    del(trips)
    gc.collect()
    

#run manually
mnths = ['2019-01-01','2019-02-01', '2019-03-01'] 
def multi_shift_load(mnths, taxi_type, rest, dirs):
    start_time = time.time()
    print('starting pool')
    pool = Pool(6)
    print('trying partial')
    run_shifts_parallelized = partial(run_shifts, taxi_type=taxi_type, rest = rest, dirs = dirs)
    #calc2 = partial(calc, b=3, c=7)
    print('mapping...')
    #print(pool.map(calc2, [1, 2, 3, 4, 5, 6]))
    pool.map(run_shifts_parallelized, mnths)
    pool.close()
    pool.join()
    te = str(time.time() - start_time)
    print('parallelized shifts processed in ' + te + ' seconds.')

#run function
if __name__ == "__main__":
    multi_shift_load(mnths, taxi_type = 'med', rest = 4, dirs = paths)
    
    
    
    
