# A data pipeline to calculate and track taxi shifts

# Purpose

It was always believed that Yellow and Green Taxis operate within a shift model,
that is drivers work 7-12 hour shifts, passing vehicles they lease to their partners after their shift ends.
We the TLC do not collect data that accurately tracks this work arrangement. We do not know when a shift starts or ends. 
I was tasked with the objective of using our existing trip data to impute shift start and end times. I was also required to build pertinent metrics around these new
definitions and track them over the best time interval I could identify. I chose to look at shifts over the course of a month as I believe this allows for overlap during weeks and 
was manageable given my computer specs.

# The Prep

I started this challenge by identifying what my result needed to be; a table of metrics tracked over month and year and couched in a driver's shift. I produced the specs below:

specs

The idea would be house this table in our SQL Server Datawarehouse -- an analytical layer I had built to host pertinent metrics that feed our policies and dashboards. Now I needed to 
think about how I might identify a shift. I decided, based of my research to look at the time between trips and see if there was a threshold at which drivers seemed to stop driving for some time. 
This would essentially demarcate a shift; this ultimately ended up being 4 hours or more -- so if a driver had a 4 or higher hour break between trips we would say they are starting a new shift. 

# The How

A task like this for us would be completed with SQL, R, Python or a combination of the sort. In our case I chose to primarily use a pythonic solution. The reason for this is that our
SQL Server receives a lot of traffic and a SQL solution seemed more complex and computationally difficult. In addition, my team and I already had an alternative data source solution;
we had created a data pipeline that recorded each day of trip records as parquet files saved into a directory so we could perform rapid calculations that could be parellalized through iteration. 
Below you can see what that looks like:  


med pic
shl pic

With our initial data source pegged and our result identified below was the general workflow I produced to capture what we needed:

1. Read a month of trip data
    1. bind the month together
    2. clean it up
2. calculate the shift
    1. order the trips
    2. calculate breaks
    3. demarcate the beginning of breaks greater than n
    4. build a unique id associated with every shift for a driver and their vehicle
3. Build metrics
    1. build metrics grouped by month, year and the unique shift
4. Load data to SQL
    1. create a table in SQL and procedures for updating data
    2. load the remaining data frame to our SQL Server
5. Automate the process
    1. set up to run once a month

# Reading a month of trip data

The projects scripts are here; but I will go over the basics of how I achieved each piece. As I work with temporal data a lot, I like functions that can take a date and run what I need, I started by writing
a function that pulls a month of data:

```
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
    #define data paths
    paths ={
        'med':'I:/COF/COF/_M3trics2/records/med_parquet',
        'shl':'I:/COF/COF/_M3trics2/records/shl_parquet'
    }
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
```


# Calculating Shifts


# Building metrics

# Loading data to SQL

# Automating the process