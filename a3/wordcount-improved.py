from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, string

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    linesep = wordsep.split(line)
    for w in linesep:
        yield (w.lower(), 1)

def filter_empty(word):
    if word != "":
        return word

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, outputs):
    text = sc.textFile(inputs).repartition(16)
    words = text.flatMap(words_once)
    words_non_empty = words.filter(filter_empty)
    wordcount = words_non_empty.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(outputs)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit-averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)