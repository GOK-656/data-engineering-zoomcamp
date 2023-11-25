import argparse

import pyspark
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()

parser.add_argument('--input_records', required=True)
parser.add_argument('--input_loc', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_records = args.input_records
input_loc = args.input_loc
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-391576947327-ctzkwzsv')

df_records = spark.read.parquet(input_records)
df_loc = spark.read.parquet(input_loc)

df_records.createOrReplaceTempView('records')
df_loc.createOrReplaceTempView('loc')

df_result = spark.sql("""
select    SNo, 
          TO_DATE(ObservationDate, 'MM/dd/yyyy') as ObservationDate, 
          r.`Province/State`, 
          r.`Country/Region`,
          CAST(Confirmed as INTEGER) as Confirmed, 
          CAST(Deaths as INTEGER) as Deaths, 
          CAST(Recovered as INTEGER) as Recovered, 
          CAST(Lat as FLOAT) as Lat, 
          CAST(Long as FLOAT) as Long
          from records r left join loc l
          on r.`Province/State` = l.`Province/State`
""")

df_result.write.format('bigquery') \
    .option('table', output) \
    .save()