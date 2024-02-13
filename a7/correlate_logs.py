import sys, re, math

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types


# add more functions as necessary
def line_split_helper(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    split_line = line_re.split(line)

    if len(split_line) == 6:
        return split_line[1], int(split_line[4])


def main(inputs):
    raw_rdd = sc.textFile(inputs)
    processed_rdd = raw_rdd.map(line_split_helper).filter(lambda x: x is not None)

    corrSchema = types.StructType([
        types.StructField("hostname", types.StringType()),
        types.StructField("num_of_bytes", types.IntegerType())
    ])

    raw_df = spark.createDataFrame(data=processed_rdd, schema=corrSchema)
    xy_df = raw_df.groupBy(raw_df.hostname).agg(functions.count(raw_df.num_of_bytes).alias('x'),
                                                functions.sum(raw_df.num_of_bytes).alias('y'))
    processed_df = xy_df.withColumn('x2', xy_df.x * xy_df.x).withColumn('y2', xy_df.y * xy_df.y). \
        withColumn('xy', xy_df.x * xy_df.y).withColumn('one', functions.lit(1))
    corr_df = processed_df.select('one', 'x', 'y', 'x2', 'y2', 'xy').groupBy().sum().collect()
    corr = (corr_df[0][0] * corr_df[0][-1] - corr_df[0][1] * corr_df[0][2]) / \
           (math.sqrt(corr_df[0][0] * corr_df[0][3] - (corr_df[0][1]) ** 2) *
            math.sqrt(corr_df[0][0] * corr_df[0][4] - (corr_df[0][2]) ** 2))
    print(f"r = {corr}")
    print(f"r^2 = {corr ** 2}")




if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('correlate-logs').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)
