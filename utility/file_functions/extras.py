from pyspark.sql.functions import *

def set_ids(cols):
   ids = sha2(concat_ws("||",*cols), 256)
   return ids

def add_time(df):
   df = df.withColumn("ingest_date", current_timestamp()).distinct()
   return df



