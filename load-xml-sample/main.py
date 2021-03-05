import os

from pyspark import SparkConf, SparkContext, SQLContext

CURRENT_DIR = os.path.dirname(__file__)

conf = (SparkConf()
        .setAppName('xml-test')
        .setMaster('local[*]'))
sc = SparkContext(conf=conf).getOrCreate()
spark = SQLContext(sc).getOrCreate(sc)

df_sample = (spark.read
             .format('xml')
             .options(rowTag='item')
             .load(os.path.join(CURRENT_DIR, 'sample.xml')))

print(df_sample.collect())

df_combined = (spark.read
               .format('xml')
               .options(rowTag='item')
               .load(os.path.join(CURRENT_DIR, 'combined.xml')))

print(df_combined.collect())
