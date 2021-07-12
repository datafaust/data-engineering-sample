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

Once I have a month's worth of trip data collected and cleaned I can then run a shift calculation on that trip data. The idea is to order the information by driver and vehicle, calculate the 
breaks between trips and demarcate when a break is greater than 4 hours. I then create a shift id unique to the first record after every 4 hour break. See the code below:

```
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
```

# Building metrics

Once I have the shifts calculated I can begin to produce aggregations based on the unique shifts for each driver. In my case I chose to preserve the orignal dataframe length as long as possible, in part because
I felt it would help me identify any incorrect calculations more rapidly, but in a less hardware-friendly enviornment I would approach this somewhat differently. Below I run a series of functions to 
produce metrics and return a dataframe with unique shifts ready to load to SQL:

```
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
```

# Loading data to SQL

With the data prepared, I need to load this into our SQL Server. Since I would be using SQLAlchemy, I decided to rely on a temp table - update model where I produced a temp table to stage data
everytime this program ran and then updated a production table via a SQL statement. The staging table would then be eliminated. I first created the tables I needed:

```
---FINAL MEDALLION SHIFT TABLE
DROP TABLE IF EXISTS medallion_monthly_shift_metrics;
CREATE TABLE  medallion_monthly_shift_metrics
(
hack    varchar(20),
med     varchar(20), 
shift_id     int,
shift_start_time  datetime,
shift_end_time    datetime,
trips_per_shift   int,
total_hours_per_shift   float,
trip_hours_per_shift float,
cruise_hours_per_shift float,
distance_per_shift float,
fare_per_shift float,
tip_per_shift float,
surcharge_per_shift float,
total_amount_per_shift float,
shift_type VARCHAR(10),
metric_weekday VARCHAR(10),
metric_month date
PRIMARY KEY (metric_month,shift_id)
);
```

Then the loading procedure in Python:

```
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
```

# Project structure

Often I see other data engineers and analysts setup a process that relies on one file that holds a lot of code with little documentation. I like to break down my projects and modularize them as 
that makes them easier to work with and much more transferable. For that reason I organized my project as such; the structure map for reference is below:
-shift_builder
--etl
--->functions.py
--->__init__.py
--sql
--->create_tables.sql
main.py


# Automating the process