from pyspark.sql.functions import *
import configparser
from pyspark import SparkConf

def set_ids(cols):
   ids = sha2(concat_ws("||",*cols), 256)
   return ids

def add_time(df):
   df = df.withColumn("ingest_date", current_timestamp()).distinct()
   return df


def get_pyspark_config():
   config=configparser.ConfigParser()
   config.read("configs/spark.conf")
   pyspark_conf=SparkConf()
   for(key,val)in config.items():
      pyspark_conf.set(key,val)
   return (pyspark_conf)
