import sys, time
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    words = path.split('/')
    hour = words[-1][11:22]
    return hour


def main(inputs, output):

    # define table schema
    weather_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType())
    ])

    # ETL
    df_raw = spark.read.csv(path=inputs, schema=weather_schema, sep=',', header=False)
    df_raw.createOrReplaceTempView('df_raw')

    df_correct = spark.sql("SELECT * FROM df_raw WHERE qflag IS NULL AND observation IN ('TMAX', 'TMIN')")
    df_correct.createOrReplaceTempView('df_correct')

    # Compute range
    df_range = spark.sql("SELECT date, station, MAX(value) - MIN(value) AS range FROM df_correct GROUP BY 1,2")
    df_range.createOrReplaceTempView('df_range')
    df_max_range = spark.sql("SELECT date, MAX(range) AS range FROM df_range GROUP BY 1")
    df_max_range.createOrReplaceTempView('df_max_range')
    df_final = spark.sql("SELECT t1.date, t2.station, t1.range / 10 AS range FROM df_max_range t1 LEFT JOIN df_range t2 ON t1.date=t2.date AND t1.range=t2.range ORDER BY 1, 2")
    # df_final.explain()

    # Save output
    df_final.write.csv(output, mode='overwrite')
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather-temp-range-sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    # time1 = time.time()
    main(inputs, output)
    # time2 = time.time()
    # print("Time cost of execution is:", time2-time1)