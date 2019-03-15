!from __future__ import print_function
import pandas as pd
import sys, re
import uuid
import os
import json
from operator import add
from pyspark.sql import SparkSession
pd.options.display.html.table_schema = True

spark = SparkSession\
  .builder\
  .appName("Gluon Results")\
  .getOrCreate()

filename = '{0}'.format( uuid.uuid4())

default_value = 'NA'

row = {}
row['host'] = os.getenv('host', default_value)
row['end'] = os.getenv('end', default_value)
row['te'] = os.getenv('te', default_value)
row['memory'] = os.getenv('memory', default_value)
row['systemtime'] = os.getenv('systemtime', default_value)
row['uniqueid'] = os.getenv('uniqueid', default_value)
row['class1'] = os.getenv('class1', default_value)
row['cpu'] = os.getenv('cpu', default_value)
row['pct1'] = os.getenv('pct1', default_value)
row['shape'] = os.getenv('shape', default_value)

json_string = json.dumps(row)
            
print(json_string)

df = spark.read.json(spark.sparkContext.parallelize([json_string]))
df.show(truncate=False)

spark.stop()
