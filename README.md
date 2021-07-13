# Purpose

The purpose of this repo is to offer examples of data engineering projects I have worked on. The repo holds examples in different languages I work with. Click the links below to read through walkthroughs:


1. [Python](https://github.com/datafaust/data-engineering-sample/tree/main/Python)
    1. **Shift Builder**: this project leverages parquet file read speeds to build out 
an ETL process that calculates shift times for taxi drivers in the green and yellow markets and uploads aggregated metrics to a SQL datawarehouse. [Click here for the walkthrough](https://github.com/datafaust/data-engineering-sample/blob/main/Python/shift_builder/walkthrough.md).
2. [R](https://github.com/datafaust/data-engineering-sample/tree/main/R)
3. [SQL](https://github.com/datafaust/data-engineering-sample/tree/main/Python)
    1. **Monthly Taxi Indicators**: this project partially reviews the process for producing part of our [public facing indicators](https://www1.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv) which are currently stored as a table in our policy datawarehouse. These indicators
    feed multiple public facing dashboards that I built like this [R Shiny Dashboard](https://tlcanalytics.shinyapps.io/tlc_fast_dash/). [Click here for the walkthrough of the SQL pipeline](https://github.com/datafaust/data-engineering-sample/blob/main/SQL/monthly_indicators/walkthrough.md) 
