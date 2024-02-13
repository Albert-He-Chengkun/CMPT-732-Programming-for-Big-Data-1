from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
   raw_string = line.split()
   date = raw_string[0]
   language = raw_string[1]
   title = raw_string[2]
   times = raw_string[3]

   if language == "en" and title != "Main_Page" and not title.startswith("Special:"):
       yield (date, (int(times), title))
   
def compare_to(x, y):
    return x if x[0] > y[0] else y

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
words = text.flatMap(words_once)
max_count = words.reduceByKey(compare_to)

outdata = max_count.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)