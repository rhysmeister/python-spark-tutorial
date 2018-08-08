from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

	'''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
	conf = SparkConf().setAppName("unionLogs").setMaster("local[*]")
	sc = SparkContext(conf = conf)

	julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")

	tagsheader = julyFirstLogs.first()
	header = sc.parallelize([tagsheader])
	julyFirstLogs = julyFirstLogs.subtract(header)

	augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

	tagsheader = augustFirstLogs.first()
	header = sc.parallelize([tagsheader])
	augustFirstLogs = augustFirstLogs.subtract(header)

	hosts1 = julyFirstLogs[0]
	hosts2 = augustFirstLogs[0]

	bothdays = hosts1.intersect(hosts2)
	bothdays.saveAsTextFile("out/nasa_logs_same_hosts.csv")
