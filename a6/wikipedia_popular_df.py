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
    wiki_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.IntegerType()),
        types.StructField('byte', types.IntegerType()),
    ])

    # ETL
    df_raw = spark.read.csv(path=inputs, schema=wiki_schema, sep=' ', header=False)\
        .withColumn("filename", functions.input_file_name())
    df_time = df_raw.withColumn("hour", path_to_hour(df_raw.filename))
    df_filter = df_time.filter((df_time.language == "en") & \
                               (df_time.title.startswith("Special:") == False) & \
                                (df_time.title != "Main_Page")).cache()
    
    df_max_views = df_filter.groupBy(df_filter.hour).agg(functions.max(df_filter.views).alias("views"))
    df_before_final = df_max_views.join(df_filter, how="left", on=["hour", "views"])
    # df_before_final = df_max_views.join(functions.broadcast(df_filter), how="left", on=["hour", "views"])
    df_final = df_before_final.select("hour", "title", "views").sort("hour")
    df_final.explain()

    # Svae output
    df_final.coalesce(1).write.json(output, mode='overwrite')
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wilipedia-popular-df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    # time1 = time.time()
    main(inputs, output)
    # time2 = time.time()
    # print("Time cost of execution is:", time2-time1)