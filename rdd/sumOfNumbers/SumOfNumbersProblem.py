import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName("unionLogs").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    data = sc.textFile("in/prime_nums.text")
    data = data.flatMap(lambda line:line.split(" "))
    data = sc.parallelize(data)
    data = data.filter(lambda number: number)
    data = data.map(lambda number: int(number))
    sum = data.reduce(lambda x, y: x + y)
    print("product is :{}".format(sum))
