"""
# ingest.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:33 PM 12/18/2021 using PyCharm
"""
import os
import pathlib

from etl.utils.logging_utils import Logger
# logger = Logger("ingest").get_logger()

class Ingest:

    def __init__(self, spark):
        self.spark = spark

    def read_csv(self, file_path):
        # logger.info("inside ingest class")
        df = self.spark.read.csv(file_path, header=True)
        return df
        # df.describe().show()
        # df.select("Country").show()
        # df.filter(col("Happiness Rank") < 10).show()
        # df.groupBy("Region").agg({"Happiness Score":"avg", "Health (Life Expectancy)":"max"}).show()