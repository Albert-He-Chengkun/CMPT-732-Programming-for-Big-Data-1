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
    df_correct = df_raw.filter(df_raw.qflag.isNull()).\
        filter((df_raw.observation == 'TMAX') | (df_raw.observation == 'TMIN'))

    # compute range
    df_range = df_correct.groupBy('date','station').\
        agg((functions.max('value') - functions.min('value')).alias('range')).cache()
    df_max_range = df_range.groupBy('date').agg(functions.max('range').alias('range'))
    df_max_range_station = df_max_range.join(df_range, how='left', on=['date', 'range'])
    df_final = df_max_range_station.select('date','station',(df_max_range_station.range / 10).alias('range')).orderBy('date','station')
    # df_final.explain()

    # Save output
    df_final.write.csv(output, mode='overwrite')
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather-temp-range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    # time1 = time.time()
    main(inputs, output)
    # time2 = time.time()
    # print("Time cost of execution is:", time2-time1)