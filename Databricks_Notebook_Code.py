#Databricks Notebook Code

import requests
import csv
import io
import pandas as pd
from pyspark.sql import functions as f
from  pyspark.sql.functions import input_file_name
#import if_common_function
import time
from datetime import datetime


years=[2020]
years_month=[]
    
for yr in years:
    yr = pd.period_range(start=f'{yr}-01',end=f'{yr}-12', freq='m')
    for years in yr:
        months=str(years.year)+"-"+ str('{:02d}'.format(years.month))
        years_month.append(months)
       
       
for fn in years_month:
    
    link = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{0}.csv'.format(fn)
    req = requests.get(link, allow_redirects=True)
    pd_df = pd.read_csv(io.StringIO(req.text))
    spark_df = spark.createDataFrame(pd_df)
    spark_df.write.option("header", "true").csv("dbfs:/filestore/shared_uploads/murli.chitlange@ica.se/yello_taxi_trip/file_{0}".format(fn))
       
        data_file=spark.read.option("header",'true').csv("dbfs:/filestore/shared_uploads/murli.chitlange@ica.se/yello_taxi_trip/*")
data_file=(
        data_file
        .withColumn("file_period", f.regexp_replace(input_file_name().substr(76,7),'-',''))
        )
        
        

data_file.write.format("delta").mode("overwrite").partitionBy("file_period").option('overwriteschema','true').save("dbfs:/filestore/shared_uploads/murli.chitlange@ica.se/yello_taxi_delta_format_file/")

spark.sql("create table if not exists yello_taxi_bronze_table (VendorID string,tpep_pickup_datetime string,tpep_dropoff_datetime string,passenger_count string,trip_distance string,RatecodeID string,store_and_fwd_flag string,PULocationID string,DOLocationID string,payment_type string,fare_amount string,extra string,mta_tax string,tip_amount string,tolls_amount string,improvement_surcharge string,total_amount string,congestion_surcharge string) using delta partitioned by (file_period string) location 'dbfs:/filestore/shared_uploads/murli.chitlange@ica.se/yello_taxi_delta_format_file/'")

top_10_route=spark.sql("select pulocationid, dolocationid,round(sum(total_amount),2) as total_amount from yello_taxi_bronze_table group by pulocationid, dolocationid order by total_amount desc limit 10")
top_10_route.write.mode("overwrite").saveAsTable("top_10_route")

location_pair_agg=spark.sql("select pulocationid, dolocationid,round(sum(total_amount),2) as total_amount from yello_taxi_bronze_table group by pulocationid, dolocationid")
location_pair_agg.write.mode("overwrite").saveAsTable("location_pair_agg")
