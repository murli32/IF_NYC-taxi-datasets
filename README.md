# IF_NYC-taxi-datasets
Assignment on NYC taxi datasets yellow taxi trip data for 2020


Attached Yellow_taxi_trip_main_2020.py contain a code to run in spark shell.Tweak in a code may needed depend on the cluster configuration.

In the same file at the downside a commented code is there which can be directly run in Notebook without any tweak to test the code job fulfillment.




EXERCISE 1
Get the raw data to a landing location
Download NYC taxi datasets from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
Fetch Yellow taxi trip data for 2020
Use a dedicated DBFS location as a storage
Make the code extensible to fetch 2021 once it's ready
Preview and verify data load completeness

EXERCISE 2
Move data from raw to bronze tables
Apply delta format
Apply monthly partitions (based on file timestamps)
Register as table

EXERCISE 3
Data aggregation (tables available to data analysts)
Use Delta format
Top 10 routes (location pairs) of highest total_amount
Calculate total_amount of for each location pair (PULoactionID, POLocationID)
Register as tables

EXERCISE 4 - DISCUSSION
How to deploy/propagate through environments?
How to schedule/orchestrate?
How to enable access to analysts?
