from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def data_formatter(commentdata):
    '''
    extract column subreddit & score
    returns: key value pair with count in tuple
    '''
    key = commentdata['subreddit']
    value = commentdata['score']
    count = 1
    return (key, (count, value))

def add_pairs(pair_a, pair_b):
    '''
    add values of pairs by position, called by reduceByKey()
    returns: the sum result in tuple
    '''
    return (pair_a[0] + pair_b[0], pair_a[1] + pair_b[1])

def average_score(pair):
    '''
    calculate average score
    returns: key value pair with the average score in tuple
    '''
    key, item = pair[0], pair[1]
    count, value = item[0], item[1]
    return (key, value / count)

def positive_filter(pair):
    '''
    return pairs with positive value only, called by filter()
    '''
    if pair[1] > 0:
        return pair
    
def relative_score_calculator(commentbysub):
    '''
    calculate relative score
    returns: key value pair of relative score & author in tuple
    '''
    subreddit = commentbysub[0]
    score = commentbysub[1][0][0]
    author = commentbysub[1][0][1]
    average_score = commentbysub[1][1]
    relative_score = score / average_score
    return (relative_score, author)


def main(inputs, output):
    # main logic starts here
    json_file = sc.textFile(inputs)
    commentdata = json_file.map(json.loads).cache()

    formatted_data = commentdata.map(data_formatter)
    sum_data = formatted_data.reduceByKey(add_pairs)
    average_data = sum_data.map(average_score)
    filtered_average_data = average_data.filter(positive_filter)

    commentbysub = commentdata.map(lambda c: (c['subreddit'], (c['score'],c['author']))).join(filtered_average_data)
    final_data = commentbysub.map(relative_score_calculator).sortBy(lambda x: x[0], ascending=False)
    final_data.map(json.dumps).saveAsTextFile(output)
    
        

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit-averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)