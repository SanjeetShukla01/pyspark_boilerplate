"""
# app.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:30 PM 12/18/2021 using PyCharm
"""
import sys
import datetime
from pyspark.sql import SparkSession
from src.etl import ingest, transform, load
from src.utils import utils
import logging
import logging.config


class Pipeline:
    logging.config.fileConfig("config/logging.conf")

    def partitioned_output_path(self, file_path):
        """
        This function provides the output path to which the data frame will be created
        :param file_path: file path from config_utils for the value 'output_file_path'
        :return: output file path with date partition
        """
        file_path = file_path + '/' + str(datetime.datetime.today().year) + '/' + str(
            datetime.datetime.today().month) + '/' + str(datetime.datetime.today().day)
        logging.info("file_path: " + file_path)
        return file_path

    def run_pipeline(self):
        try:
            input_path = utils.get_config("IO_CONFIGS", "INPUT_DATA_PATH")
            ingest_process = ingest.Ingest(self.spark)
            df = ingest_process.read_csv(input_path + "/2015_world_happiness.csv")
            df.show()

            transform_process = transform.Transform(self.spark)
            tdf = transform_process.transform_data(df)
            tdf.show()

            output_path = utils.get_config("IO_CONFIGS", "OUTPUT_DATA_PATH")
            persist_process = load.Persist(self.spark)
            persist_process.dump_data(tdf, self.partitioned_output_path(output_path))
        except Exception as exp:
            logging.error("An error occurred while running the pipeline > " + str(exp))
            # send email notification or log to database
            sys.exit(1)

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("myPySparkApp")\
            .enableHiveSupport()\
            .getOrCreate()


if __name__ == "__main__":
    logging.info("running pipeline")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()


