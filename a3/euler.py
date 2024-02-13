from pyspark import SparkConf, SparkContext
import sys, operator, time
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import random

# add more functions as necessary
def euler(samples):
    random.seed(2023)
    total_iterations = 0
    for i in samples:
        sum = 0.0
        while(sum < 1):
            sum += random.random()
            total_iterations += 1
    
    return total_iterations


def main(samples):
    # main logic starts here
    rdd = sc.range(samples, numSlices = 10).glom()
    euler_rdd = rdd.map(euler)
    sum_result = euler_rdd.reduce(operator.add)
    euler_num = sum_result / samples
    print("Estimated Eular Constant is: {}".format(euler_num))
    
    
        

if __name__ == '__main__':
    conf = SparkConf().setAppName('Euler Simulation')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    time_a = time.time()
    samples = int(sys.argv[1])
    main(samples)
    time_b = time.time()
    print("Time taken: {}".format(time_b - time_a))