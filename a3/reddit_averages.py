from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def data_formatter(json_file):
    fmt_str = json.loads(json_file)
    key = fmt_str['subreddit']
    value = fmt_str['score']
    count = 1
    return [key, [count, value]]

def add_pairs(pair_a, pair_b):
    return (pair_a[0] + pair_b[0], pair_a[1] + pair_b[1])

def average_score(pair):
    key, item = pair[0], pair[1]
    count, value = item[0], item[1]
    return (key, value / count)

def main(inputs, output):
    # main logic starts here
    json_file = sc.textFile(inputs)
    formatted_data  = json_file.map(data_formatter)
    sum_data = formatted_data.reduceByKey(add_pairs)
    final_data = sum_data.map(average_score)
    dumped_data = final_data.map(json.dumps)
    dumped_data.saveAsTextFile(output)
    
        

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit-averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)