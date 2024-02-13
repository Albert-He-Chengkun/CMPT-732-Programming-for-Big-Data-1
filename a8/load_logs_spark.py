import os, sys, re, uuid, gzip, time
from cassandra.cluster import Cluster
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions
from datetime import datetime


# add more functions as necessary
def line_split_helper(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    split_line = line_re.split(line)

    if len(split_line) == 6:
        return (str(uuid.uuid4()), split_line[1], datetime.strptime(split_line[2],'%d/%b/%Y:%H:%M:%S'), split_line[3], int(split_line[4]))


def main(input_dir, keyspace, table_name):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 2}}")
    session.execute(f"USE {keyspace}")
    session.execute("CREATE TABLE IF NOT EXISTS nasalogs ( id UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (host, id) )")
    session.execute(f"TRUNCATE {table_name}")
    session.shutdown()

    schema = "id STRING, host STRING, datetime TIMESTAMP, path STRING, bytes INT"
    raw_rdd = sc.textFile(input_dir)
    processed_rdd = raw_rdd.map(line_split_helper).filter(lambda x: x is not None)
    raw_df = spark.createDataFrame(data=processed_rdd, schema=schema)
    raw_df.write.mode("append").format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).save()


if __name__=="__main__":
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']

    spark = SparkSession.builder.appName("load-logs-spark").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '3.0'
    sc = spark.sparkContext
    # t1 = time.time()
    main(input_dir, keyspace, table_name)
    # print(time.time() - t1)