from configs.config import *
from pyspark.sql import SparkSession
from utility.logger import *

def get_spark_session():
    spark = SparkSession \
        .builder \
        .appName('Loan Score') \
        .config(map=properties_spark) \
        .master('local[*]') \
        .getOrCreate()


    logger.info('Spark session created {}'.format(spark))

    return spark

# .config(map=properties_spark) \
# def set_properties():
#     spark_conf = SparkConf()
#     spark_conf.setAll(pairs = list(properties_spark.items()))
#     return spark_conf


