import sys, json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, DataFrameReader


def main(inputs, output):
    # Create DataFrame schema
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])
    # Import weather data
    weather = spark.read.csv(inputs, schema=observation_schema)
    # ETL 
    weather = weather.filter(weather.qflag.isNull())
    weather = weather.filter(weather.station.startswith('CA'))
    weather = weather.filter(weather.observation == 'TMAX')
    # weather = weather.select('station', 'date', (weather.value / 10).alias('tmax'))
    # save data
    weather.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather ETL S3 select').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
