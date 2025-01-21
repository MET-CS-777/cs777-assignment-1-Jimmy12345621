
from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True

    except:
        return False


# Function - Cleaning
# For example, remove lines if they don’t have 16 values and
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles,
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if (len(p) == 17):
        if (isfloat(p[5]) and isfloat(p[11])):
            if (float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0):
                return p
import data
path = '/Users/11719/Documents'
testFile = path + '/taxi-data-sorted-small.csv'
testDataFrame = sqlContext.read.format('csv').options(header='false', inferSchema='true', sep=',').load(testFile)
testDataFrame.show()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)

    spark_conf=SparkConf().setAppName("Assignment-1")
    sc = SparkContext.getOrCreate(conf=spark_conf)

    rdd = sys.argv[1]
    testDataFrame = sqlContext.read.format('csv').options(header='false', inferSchema='true', sep=',').load(rdd)

    # Task 1
    # Your code goes here
    testRDD = testDataFrame.rdd.map(tuple)
    taxilinesCorrected = testRDD.filter(correctRows) #Use the function to select only corrected rows
    lines_1 = taxilinesCorrected.map(lambda row:(row[0],row[1])) #Choose only taxi ID and driver ID
    lines_1_distinct = lines_1.distinct() #Choose only distinct taxi and driver ID pairs
    result_count =  lines_1_distinct.countByKey() #Group by taxi ID, and count driver IDs
    result_count_rdd = sc.parallelize(result_count.items())
    results_1 = result_count_rdd.takeOrdered(10,key=lambda x: -x[1]) #Top 10
    results_1 = sc.parallelize(results_1)
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])

    sc.stop()