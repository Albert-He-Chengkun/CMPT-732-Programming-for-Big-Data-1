from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def col_extractor(json_rdd):
    # extract three columns from original data
    return json_rdd['subreddit'], json_rdd['score'], json_rdd['author']

def positive_filter(data_bucket):
    # filter out data defined as 'positive' & has 'e' in its subreddit
    subreddit, score, author = data_bucket
    if 'e' in subreddit and score > 0:
        return subreddit, score, author
    
def negative_filter(data_bucket):
    # filter out data defined as 'negative' & has 'e' in its subreddit
    subreddit, score, author = data_bucket
    if 'e' in subreddit and score <= 0:
        return subreddit, score, author

def main(inputs, output):
    # main logic starts here
    json_file = sc.textFile(inputs)
    json_rdd  = json_file.map(json.loads)
    extracted_cols = json_rdd.map(col_extractor).cache()

    positive_rdd = extracted_cols.filter(positive_filter)
    negative_rdd = extracted_cols.filter(negative_filter)

    positive_rdd.map(json.dumps).saveAsTextFile(output + '/positive')
    negative_rdd.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit-etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)