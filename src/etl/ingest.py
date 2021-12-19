"""
# ingest.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:33 PM 12/18/2021 using PyCharm
"""
import logging
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import logging.config


class Ingest:
    logging.config.fileConfig("config/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_source_data(self):
        print("extracting source input_data")
        my_list = [1, 2, 3]
        df = self.spark.createDataFrame(my_list, IntegerType())
        df.show()

    def read_csv(self, file_path):
        df = self.spark.read.csv(file_path, header=True)
        return df
        # df.describe().show()
        # df.select("Country").show()
        # df.filter(col("Happiness Rank") < 10).show()
        # df.groupBy("Region").agg({"Happiness Score":"avg", "Health (Life Expectancy)":"max"}).show()