import configparser

from pyspark import SparkConf
from pyspark.sql import *

from logger import Log4J



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    print_hi('PyCharm')


if __name__ == '__main__':
    conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")
    for k, v in config.items("SPARK_APP_CONFIGS"):
        conf.set(k, v)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4J(spark)
    logger.loger.warn("hello")
    data = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "/Users/pankajpachori/PycharmProjects/sparklearning/data/bd-dec22-births-by-mothers-age.csv")
    dataone = data.repartition(2)
    input("press enter")
    groupdata = dataone.groupby("Period")
    conf_out = spark.sparkContext.getConf()
    logger.loger.warn(f'finish {groupdata.count().show()}')





