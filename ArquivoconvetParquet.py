# Databricks notebook source
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark import SparkConf, SparkContext
from operator import add

file_location = "/FileStore/tables/ArquivoTeste.csv"
file_location_return = "/FileStore/tables/ArquivoTeste.parquet"
file_type = "csv"


infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)
print('teste %s' % df)
df.write.parquet(file_location_return)


sc.stop()

