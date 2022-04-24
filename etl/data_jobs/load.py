"""
# load.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:33 PM 12/18/2021 using PyCharm
"""
import logging
import os
import pathlib

from pyspark.sql.dataframe import DataFrame
import logging.config

from etl.utils.logging_utils import Logger
logger = Logger("load").get_logger()


class Persist:

    def __init__(self, spark):
        self.spark = spark

    def dump_data(self, df: DataFrame, path: str) -> None:
        try:
            logger.info("persisting data")
            """Collect input_data locally and write to CSV."""
            # df.write.option("header", "true").csv(path)
            df.coalesce(1).write.mode("overwrite").csv(path, header=True)
            logger.info("csv file created")
        except Exception as exp:
            logger.error("An error occured while persisting data > " + str(exp))
            raise Exception("HDFS Directory Already Exists")

