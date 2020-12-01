from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import types
from pyspark.sql.types import StructType
from pyspark.sql.functions import avg, count
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, help="Days for moving average, default 7", default=7)
parser.add_argument("bucket", help="Bucket name to store data temp data from Dataproc to Bigquery")
parser.add_argument("input", help="GCS Path for input data. E.g. gs://bucket/file.csv")
parser.add_argument("output", help="BigQuery Table Output project:dataset:table")
args = parser.parse_args()

DAYS = (-1)*args.days
FILTER = args.days
if DAYS >= 0:
    raise argparse.ArgumentTypeError('Days has to be >= 1')
BUCKET_NAME = args.bucket
INPUT = args.input
OUTPUT = args.output

bucket = BUCKET_NAME
input_schema = StructType() \
    .add('ticker',types.StringType(),False) \
    .add('open', types.DoubleType(), False) \
    .add('close', types.DoubleType(), False) \
    .add('adj_close', types.DoubleType(), False) \
    .add('low', types.DoubleType(), False) \
    .add('high', types.DoubleType(), False) \
    .add('volume', types.LongType(), False) \
    .add('date', types.DateType(), False)

spark = SparkSession.builder.appName("Moving_Average").getOrCreate()
spark.conf.set('temporaryGcsBucket', bucket)
df = spark.read.option("header",True).schema(input_schema).csv(INPUT)

df_selected = df.select('ticker', 'close', 'date').orderBy('ticker', 'date')
w = Window.partitionBy('ticker').orderBy('date').rowsBetween(DAYS,-1)

output = df_selected.withColumn('moving_average', avg('close').over(w))
output = output.withColumn('count', count('close').over(w))
output = output.where(output['count'] == FILTER)
output = output.drop('count')

output.write.mode("overwrite").format('bigquery').option('table', OUTPUT).save()
