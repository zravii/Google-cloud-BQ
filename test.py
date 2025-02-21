from pyspark.sql import SparkSession
import random

spark = SparkSession.builder \
    .appName("PythonPi") \
    .getOrCreate()

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

NUM_SAMPLES = 1000000

count = spark.sparkContext.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()

pi_estimate = 4.0 * count / NUM_SAMPLES
print("Pi is roughly", pi_estimate)

spark.stop()
