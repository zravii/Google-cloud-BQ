from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("FlightDataPreprocessing") \
    .getOrCreate()

try:

    data = spark.read.csv('gs://flight_data_buck/T_T100I_SEGMENT_ALL_CARRIER.csv', header=True)

    #Checking non-negative numeric fields
    data = data.filter((col('DISTANCE') >= 0) & (col('PAYLOAD') >= 0) & (col('PASSENGERS') >= 0))

    #Verifing that essential fields are not null
    essential_fields = ['DEPARTURES_SCHEDULED', 'DEPARTURES_PERFORMED', 'DISTANCE', 'UNIQUE_CARRIER', 'ORIGIN', 'DEST']
    for field in essential_fields:
        data = data.filter(col(field).isNotNull())

    #Removing duplicate records
    data = data.dropDuplicates()

    #Columns to keep
    columns_to_keep = ['DISTANCE', 'PAYLOAD', 'SEATS', 'PASSENGERS', 'FREIGHT', 'MAIL', 'RAMP_TO_RAMP', 'AIR_TIME', 'DEPARTURES_SCHEDULED', 'DEPARTURES_PERFORMED', 'UNIQUE_CARRIER_NAME', 'REGION', 'ORIGIN_CITY_NAME', 'ORIGIN_COUNTRY_NAME', 'DEST_CITY_NAME', 'DEST_COUNTRY_NAME', 'AIRCRAFT_GROUP', 'AIRCRAFT_TYPE', 'AIRCRAFT_CONFIG', 'DISTANCE_GROUP', 'CLASS']

    #Retain specified columns
    data = data.select(*columns_to_keep)

    #Check if any columns have the same value for all rows
    for col_name in data.columns:
        unique_values = data.select(col_name).distinct().collect()
        if len(unique_values) == 1:
            raise ValueError(f"Column '{col_name}' has the same value for all rows")

    #Export to BigQuery
    data.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", "flight_data_buck") \
        .option("table", "sp24-i535-ravishar-flightdata.dataset_flight.clean_data") \
        .mode("overwrite") \
        .save()

    print("Complete")

except Exception as e:
    #Print error message if any error occurs
    print("Error:", e)

finally:
    spark.stop()
