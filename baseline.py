from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
spark = SparkSession.builder.appName('taxi_baseline').getOrCreate()

taxi_folder = "s3://aws-ai-gds-dev/spark/assignment/ny_yellow_taxi/taxi/"

# Spark's built-in Hadoop file system tool lists files in S3 (no extra install needed on EMR)
hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(taxi_folder)
fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())

taxi_files = []
for f in fs.listStatus(hadoop_path):
    path = str(f.getPath())
    if path.endswith(".parquet"):
        # file name ends like ...2024-11.parquet -> take "2024-11"
        year_month = path[-15:-8]
        year, month = year_month.split("-")
        taxi_files.append((path, int(year), int(month)))


# ---------- 2. Columns we keep, and the type for each ----------
# Every file is forced into this same list, so files from different years combine cleanly.
# If a file doesn't have a column (e.g. cbd_congestion_fee before 2025), it's filled with null.
columns = {
    "tpep_pickup_datetime":  "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "payment_type":          "int",
    "fare_amount":           "double",
    "tip_amount":            "double",
    "passenger_count":       "int",
    "trip_distance":         "double",
    "PULocationID":          "int",
    "DOLocationID":          "int",
    "tolls_amount":          "double",
    "total_amount":          "double",
    "cbd_congestion_fee":    "double",
}

## 3rd
taxi_df = None

for path, year, month in taxi_files:
    df = spark.read.format('parquet').load(path)

    # lowercase copy of this file's column names, used only for the check
    file_cols_lower = [c.lower() for c in df.columns]

    # Build the fixed column list for this file
    selected = []
    for name, dtype in columns.items():
        if name.lower() in file_cols_lower:
            # column exists (any case): cast it, keep dictionary spelling
            selected.append(F.col(name).cast(dtype).alias(name))
        else:
            # column missing: null
            selected.append(F.lit(None).cast(dtype).alias(name))

    df = (df.select(selected)
            .withColumn("file_year", F.lit(year))      # remember which month this file covers
            .withColumn("file_month", F.lit(month)))

    # Stack this file under the previous ones
    taxi_df = df if taxi_df is None else taxi_df.unionByName(df)


# ---------- 4.few additional columns ----------
#
intermediate_taxi_df = (taxi_df
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .drop("file_year", "file_month"))


# ---------- 5. Lookup table ----------  
lookup_df = (spark.read.format('csv')
             .option("header", "true")        # first row has column names
             .option("inferSchema", "true")   # read LocationID as a number
             .load("s3://aws-ai-gds-dev/spark/assignment/ny_yellow_taxi/lookup/*.csv"))




###Transformations -- 5 
## wide transformation
joined_table = intermediate_taxi_df.alias('taxi').join(lookup_df.alias('lookup'),on=F.col("taxi.PULocationID")==F.col("lookup.LocationID"),how='inner')

### after wide transformation we using filter
cleaned_joined_table=(joined_table.filter(F.col("fare_amount") > 0)                                   # 1. positive fare
        .filter(F.col("total_amount") > 0)                                  # 2. positive total
        .filter(F.col("passenger_count") > 0)                               # 3. at least 1 passenger (also removes nulls)
        .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100))   # 4. distance between 0 and 100 miles
        .filter((F.year("tpep_pickup_datetime") == F.col("file_year")) &          # 5. pickup inside the file's month
            (F.month("tpep_pickup_datetime") == F.col("file_month")))
)






#5.1

revenue_and_demand = (cleaned_joined_table.groupBy('borough','zone').agg(
                                                        F.count("*").alias('trips_count'),
                                                        F.round(F.sum('total_amount'),2).alias('total_revenue'),
                                                        F.round(F.avg('trip_distance'),2).alias('average_trip_distance'),
                                                        F.round(F.avg('fare_amount'),2).alias('average_fare_amount'),
                                                        F.round(F.avg(F.col('tip_amount')/F.col('fare_amount')),2).alias('average_tip_ratio')
                                                        )
                                                    )

##5.2 lag -2

w=Window.partitionBy('zone').orderBy('pickup_hour').rangeBetween(-2,0)

hourly_demand = (cleaned_joined_table.groupBy('zone','pickup_hour')
                            .agg(F.count("*").alias('trips')))

hourly_demand_windowed = (hourly_demand
                            .withColumn('trips_moving_avg_3h',F.round(F.avg('trips').over(w),2)))

#5.3 inter trip gap per zone


w1 = Window.partitionBy('zone').orderBy('tpep_pickup_datetime')

intermediate_df = (cleaned_joined_table.withColumn('previous_time',F.lag('tpep_pickup_datetime',1).over(w1))
                               .withColumn('gap',F.round((F.col('tpep_pickup_datetime').cast('long')-F.col('previous_time').cast('long'))/60,2)))



gap_per_zone  = (intermediate_df.filter(F.col('gap').isNotNull()).groupBy('zone').agg(
                                                                                      F.count('*').alias('trip_count'),
                                                                                      F.percentile('gap',0.5).alias('median_gap_min'),
                                                                                      F.percentile('gap',0.9).alias('p90_gap_min')))            
                            

##5.4 Tipping behavior segmentation
@F.udf (returnType=T.StringType())
def distance_bucket (distance):
    if distance is None:
        return None
    if distance < 2:
        return '[0,2)'
    if distance <5 :
        return '[2,5)'
    if distance <10:
        return '[5,10)'
    return '[10,100]'

tipping_behaviour = (cleaned_joined_table.filter(F.col('payment_type')==1)
                 .withColumn('distance_band',distance_bucket('trip_distance')) 
                 .withColumn('tip_ratio',F.round(F.col('tip_amount')/F.col('fare_amount'),2))                                              
                 .groupBy('payment_type','distance_band')
                 .agg(
                    F.percentile('tip_ratio',.25).alias('p25'),
                    F.percentile('tip_ratio',.50).alias('p50'),
                    F.percentile('tip_ratio',.75).alias('p75'),
                    F.percentile('tip_ratio',.90).alias('p90'),
                    F.count('*').alias('segment_trips_count')
                 )
            )

##5.5 