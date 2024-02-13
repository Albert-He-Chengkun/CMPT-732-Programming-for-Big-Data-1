import sys, time
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions


def main(topic):
    messages = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
    .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    cols = functions.split(values['value'], ' ')

    df = values.withColumn('x', cols.getItem(0)).withColumn('y', cols.getItem(1))
    df = df.select('x','y').withColumn('x2', df.x * df.x).withColumn('one',functions.lit(1)).withColumn('xy', df.x * df.y)
    df_sum = df.select(*[functions.sum(col_name).alias(col_name) for col_name in df.columns])

    beta_est = (df_sum.xy - 1 / df_sum.one * df_sum.x * df_sum.y) / (df_sum.x2 - 1 / df_sum.one * df_sum.x**2)
    alpha_est = df_sum.y / df_sum.one - df_sum.x * beta_est / df_sum.one

    df_result = df_sum.withColumn('beta_est', beta_est) \
        .withColumn('alpha_est', alpha_est).drop('one', 'x', 'y', 'x2', 'xy')
    stream = df_result.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(100)



if __name__=="__main__":
    topic= sys.argv[1]
    spark = SparkSession.builder.appName("read-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '3.0'
    sc = spark.sparkContext
    # t1 = time.time()
    main(topic)
    # print(time.time() - t1)
