"""
# app.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 4:26 PM 4/24/2022 using PyCharm
"""
import sys
import datetime
from pyspark.sql import SparkSession
from etl.data_jobs import ingest, transform, load
from etl.utils import utils
from etl.utils.logging_utils import Logger

logger = Logger("main").get_logger()


class Pipeline:

    logger.info("starting pipeline")

    def partitioned_output_path(self, file_path):
        """
        This function provides the output path to which the data frame will be created
        :param file_path: file path from config_utils for the value 'output_file_path'
        :return: output file path with date partition
        """
        file_path = file_path + '/' + str(datetime.datetime.today().year) + '/' + str(
            datetime.datetime.today().month) + '/' + str(datetime.datetime.today().day)
        logger.info("file_path: " + file_path)
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
            logger.error("An error occurred while running the pipeline > " + str(exp))
            # send email notification or log to database
            sys.exit(1)

    def create_spark_session(self):
        logger.info("creating spark session ")
        self.spark = SparkSession.builder\
            .appName("myPySparkApp")\
            .enableHiveSupport()\
            .getOrCreate()
        logger.info("spark session created")
        return self.spark


if __name__ == "__main__":
    logger.info("running pipeline")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()