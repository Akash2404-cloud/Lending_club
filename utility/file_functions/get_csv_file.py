# from utility.creating_spark_session import *
from configs.config import *

def get_file(spark , loc , inferschema , schema):
    if inferschema == True:
        basic_properties_csv['inferSchema'] = inferschema
    else:
        basic_properties_csv['schema'] = schema

    df = spark.read.csv(loc, **basic_properties_csv)
    return df

