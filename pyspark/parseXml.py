import sys

from pyspark.sql import SparkSession

xmlFIlePath = "/home/ashen/Desktop/GSB-Project/Client-Documents/NEW-GSB-FILES/Inputs/asdas/RC11_20210120234500/PM202101202330+110024LNBTS.xml"

spark = SparkSession.builder\
    .appName("XMLParseApp")\
    .master("spark://ashen-ubuntu-os:7077")\
    .getOrCreate()

df = spark.read.format("com.databricks.spark.xml").option("rowTag", "measInfo").load(xmlFIlePath).cache()

dfRows = df.count()

print("df row count: " + str(dfRows))
