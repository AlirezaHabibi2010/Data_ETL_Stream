# Data ETL

We have provided a data simulator. The simulator writes 3 records per second into a table in
PostgresSQL called devices. The structure of the table is the following:

|Property Name|Type| Comment|
|---|---|---|
|device_id|UUID|The unique ID of the device  sending the data.|
|temperature|Integer|The temperature measured by the device.|
|location|JSON|Latitude and Longitude of the position of the device.|
|time|Timestamp|The time of the signal as a Unix timestamp|




## Running the docker

To get started run ``` docker-compose up ``` in root directory.
- It will create the PostgresSQL database and start generating the data.
- It will create an empty MySQL database.
- It will launch the analytics.py script.

# ETL
The analytics.py file creates an ETL pipeline does the following:
- Pull the data from PostgresSQL
- Calculate the following data aggregations:
    1. The maximum temperatures measured for every device per hours.
    2. The amount of data points aggregated for every device per hours.
    3. Total distance of device movement for every device per hours.
- Store this aggregated data into the provided MySQL database




The structure of the MySQL table (*'devices_agg'*) is the following:
|Property Name Data Type Data|Type| Comment|
|---|---|---|
|device_id|UUID|The unique ID of the device  sending the data.|
|hour|Integer|The time of the signal as a Unix timestamp (hour base).|
|max_temperature|Integer|The max temperature measured by the device per hour.|
|data_point_pre_hour|Integer|The amount of data points measured by the device per hour.|
|total_distance_in_km_per_hour|Float|The total distance of the device movement per hour|
