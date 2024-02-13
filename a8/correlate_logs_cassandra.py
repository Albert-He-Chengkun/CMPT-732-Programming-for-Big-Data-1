import os, sys, re, uuid, gzip, time, math
from cassandra.cluster import Cluster
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions
from datetime import datetime


def main(keyspace, table_name):

    raw_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).load()
    xy_df = raw_df.groupBy(raw_df.host).agg(functions.count(raw_df.bytes).alias('x'),
                                                functions.sum(raw_df.bytes).alias('y'))
    processed_df = xy_df.withColumn('x2', xy_df.x * xy_df.x).withColumn('y2', xy_df.y * xy_df.y). \
        withColumn('xy', xy_df.x * xy_df.y).withColumn('one', functions.lit(1))
    corr_df = processed_df.select('one', 'x', 'y', 'x2', 'y2', 'xy').groupBy().sum().collect()
    corr = (corr_df[0][0] * corr_df[0][-1] - corr_df[0][1] * corr_df[0][2]) / \
           (math.sqrt(corr_df[0][0] * corr_df[0][3] - (corr_df[0][1]) ** 2) *
            math.sqrt(corr_df[0][0] * corr_df[0][4] - (corr_df[0][2]) ** 2))
    print(f"r = {corr}")
    print(f"r^2 = {corr ** 2}")




if __name__=="__main__":
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']

    spark = SparkSession.builder.appName("correlate-logs-cassandra").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '3.0'
    sc = spark.sparkContext
    # t1 = time.time()
    main(keyspace, table_name)
    # print(time.time() - t1)