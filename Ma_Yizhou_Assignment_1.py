from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


# Function - Cleaning
# Remove lines if they don’t have 16 values and apply validity checks
def correctRows(p):
    if len(p) == 17:
        if (isfloat(p[5]) and isfloat(p[11])):  # trip distance and fare amount
            if (
                float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0
            ):  # trip duration, distance, fare, and total amount
                return p


# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <input_file> <output_task1> <output_task2>", file=sys.stderr)
        exit(-1)

    # Initialize Spark Context
    sc = SparkContext(appName="Assignment-1")
    spark = SparkSession(sc)

    # Load dataset
    rdd = sc.textFile(sys.argv[1])

    # Clean the data
    clean_rdd = rdd.map(lambda x: x.split(",")).filter(correctRows)

    # Task 1: Top-10 Active Taxis
    # Extract medallion (Taxi ID) and hack_license (Driver ID)
    medallion_driver_pairs = clean_rdd.map(lambda x: (x[0], x[1]))
    medallion_driver_count = (
        medallion_driver_pairs.distinct()  # Unique (medallion, driver)
        .map(lambda x: (x[0], 1))  # Map medallion to 1
        .reduceByKey(add)  # Count distinct drivers per medallion
    )
    top_10_taxis = medallion_driver_count.takeOrdered(10, key=lambda x: -x[1])  # Top 10
    results_1 = sc.parallelize(top_10_taxis)
    # Save Task 1 output
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])
    # Task 2: Top-10 Best Drivers
    # Extract hack_license (Driver ID), trip time in seconds, and total amount
    driver_earnings = clean_rdd.map(
        lambda x: (x[1], (float(x[16]), float(x[4]) / 60))  # Total amount and trip duration in minutes
    )
    driver_avg_earnings = (
        driver_earnings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Sum total earnings and durations
        .mapValues(lambda x: x[0] / x[1])  # Calculate earnings per minute
    )
    top_10_drivers = driver_avg_earnings.takeOrdered(10, key=lambda x: -x[1])  # Top 10
    results_2 = sc.parallelize(top_10_drivers)
    # Save Task 2 output
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])
    sc.stop()
