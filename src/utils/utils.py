"""
# data_utils for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 12:15 AM 12/19/2021 using PyCharm
"""
import configparser
import logging.config
from pyspark.sql import DataFrame, Row
import os.path


def get_parent(path, levels=1):
    common = path
    for i in range(levels + 1):
        # Starting point
        common = os.path.dirname(common)

    # Parent directory upto specified
    # level
    path_s = os.path.relpath(path, common)
    return path.replace(path_s, "")


def dump_data(df: DataFrame, path: str) -> None:
    """Collect input_data locally and write to CSV."""
    df.write.csv(path, mode="overwrite", header=True)


def get_config(config_section: str, config_value: str):
    config = configparser.ConfigParser()
    config.read("config/pipeline.cfg")
    return config.get(config_section, config_value)


class Utils:
    logging.config.fileConfig("config/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def create_test_data(self) -> DataFrame:
        """Create test input_data.
        This function creates both both pre- and post- transformation input_data
        saved as Parquet files in tests/test_data. This will be used for
        unit tests as well as to load as part of the example ETL job.
        """
        # create example input_data from scratch
        local_records = [
            Row(id=1, first_name="Dan", second_name="Germain", floor=1),
            Row(id=2, first_name="Dan", second_name="Sommerville", floor=1),
            Row(id=3, first_name="Alex", second_name="Ioannides", floor=2),
            Row(id=4, first_name="Ken", second_name="Lai", floor=2),
            Row(id=5, first_name="Stu", second_name="White", floor=3),
            Row(id=6, first_name="Mark", second_name="Sweeting", floor=3),
            Row(id=7, first_name="Phil", second_name="Bird", floor=4),
            Row(id=8, first_name="Kim", second_name="Suter", floor=4),
        ]

        return self.spark.createDataFrame(local_records)

    def read_data_from_excel(self, file_path, sheet_name):
        """
        This method is intended to create a dataframe form excel file
        """
        return (
            self.spark.read.format("com.crealytics.spark.excel")
            .option("useHeader", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", "true")
            .option("addColorColumns", "False")
            .option("maxRowsInMey", 2000)
            .option(sheet_name, "Import")
            .load(file_path)
        )

    def read_csv(self, file_path):
        df = self.spark.read.csv(file_path, header=True)
        return df
