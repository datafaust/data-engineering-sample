-----------------------------------CREATE FHV TABLE
CREATE TABLE data_reports_monthly_indicators_fhv (
Month_Year VARCHAR(7),	
License_Class VARCHAR(30),	
Trips_Per_Day int, 	
Farebox_Per_Day int,	
Unique_Drivers int,	
Unique_Vehicles int,	
Vehicles_Per_Day int,	
Avg_Days_Vehicles_on_Road decimal(2,1), 	
Avg_Hours_Per_Day_Per_Vehicle decimal(2,1), 	
Avg_Days_Drivers_on_Road decimal(2,1), 	
Avg_Hours_Per_Day_Per_Driver decimal(2,1), 		
Avg_Minutes_Per_Trip	decimal(2,1), 	
Percent_of_Trips_Paid_with_Credit_Card	decimal(2,1), 	
Trips_Per_Day_Shared int
PRIMARY KEY(Month_Year, License_Class)