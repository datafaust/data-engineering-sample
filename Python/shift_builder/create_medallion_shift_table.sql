----FINAL MEDALLION TABLE
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


----FINAL SHL TABLE
----FINAL TABLE
DROP TABLE IF EXISTS shl_monthly_shift_metrics;
CREATE TABLE  shl_monthly_shift_metrics
(
hack    varchar(20),
shl     varchar(20), 
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


--pull table sample
select top 5 * from medallion_monthly_shift_metrics_temp;


