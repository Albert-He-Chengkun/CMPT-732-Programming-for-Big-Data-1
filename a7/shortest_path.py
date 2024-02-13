import sys, re, math

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window


# add more functions as necessary
def path_layout(line):
    line_split = line.split(':')
    src = line_split[0]
    des = line_split[1].split(' ')
    for node in des:
        if node != '':
            yield int(src), int(node)


def main(inputs, output, source, destination):
    raw_rdd = sc.textFile(inputs + '/links-simple-sorted.txt')
    processed_rdd = raw_rdd.flatMap(path_layout)

    pathSchema = types.StructType([
        types.StructField("source", types.IntegerType()),
        types.StructField("node", types.IntegerType())
    ])
    shortestSchema = types.StructType([
        types.StructField("kp_node", types.IntegerType()),
        types.StructField("source", types.IntegerType()),
        types.StructField("distance", types.IntegerType())
    ])

    edge_df = spark.createDataFrame(data=processed_rdd, schema=pathSchema).cache()
    known_paths_df = spark.createDataFrame(data=[(source, 0, 0)], schema=shortestSchema)

    for curr_distance in range(6):
        curr_path_df = known_paths_df.join(edge_df, known_paths_df['kp_node'] == edge_df['source'], 'inner')
        curr_path_df = curr_path_df.select(curr_path_df.node.alias('kp_node'), curr_path_df.kp_node.alias('source'),
                                           (curr_path_df.distance + 1).alias('distance'))
        known_paths_df = known_paths_df.unionAll(curr_path_df). \
            withColumn("rn", functions.rank().over(Window.partitionBy("kp_node").orderBy("distance")))
        known_paths_df = known_paths_df.filter(known_paths_df.rn == 1).select("kp_node", "source", "distance")

        known_paths_df.write.csv(output + '/iter-' + str(curr_distance), mode='overwrite')
        if known_paths_df.where(known_paths_df['kp_node'] == destination).count() > 0:
            break

    path = [destination]
    curr_node = destination
    while curr_node != source:
        curr_node = known_paths_df.where(known_paths_df['kp_node'] == curr_node).collect()[0][1]
        path.insert(0, curr_node)
    path_rdd = sc.parallelize(path)
    path_rdd.saveAsTextFile(output + '/path')
    for node in path:
        print(node)




if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    destination = int(sys.argv[4])
    spark = SparkSession.builder.appName('shortest-path').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source, destination)
