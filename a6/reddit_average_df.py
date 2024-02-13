import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):

    # define table schema
    reddit_schema = types.StructType([
        types.StructField('archived', types.BooleanType()),
        types.StructField('author', types.StringType()),
        types.StructField('body', types.StringType()),
        types.StructField('controversiality', types.IntegerType()),
        types.StructField('created_utc', types.StringType()),
        types.StructField('downs', types.IntegerType()),
        types.StructField('edited', types.StringType()),
        types.StructField('gilded', types.IntegerType()),
        types.StructField('id', types.StringType()),
        types.StructField('link_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('parent_id', types.StringType()),
        types.StructField('retrieved_on', types.IntegerType()),
        types.StructField('score', types.IntegerType()),
        types.StructField('score_hidden', types.BooleanType()),
        types.StructField('subreddit', types.StringType()),
        types.StructField('subreddit_id', types.StringType()),
        types.StructField('ups', types.IntegerType()),
        types.StructField('month', types.IntegerType())
    ])

    # read & operate
    df_raw = spark.read.json(inputs, schema=reddit_schema)
    df_source = df_raw.select("subreddit", "score")
    df_result = df_source.groupBy("subreddit").agg(functions.avg('score').alias('average_score'))
    # df_result.explain()

    # save
    df_result.write.csv(output, mode='overwrite')
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)