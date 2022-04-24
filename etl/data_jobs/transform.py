"""
# transform.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:33 PM 12/18/2021 using PyCharm
"""
from etl.utils import column_constants
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from etl.utils.logging_utils import Logger

logger = Logger("transform").get_logger()


class Transform:

    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logger.info("Doing some data transformation")
        columns = getattr(column_constants, "column_constants")

        tdf = df.filter(col(columns["HAPPINESS_RANK"]) < 20)\
            .groupBy("Region").agg({"Happiness Score": "avg", "Health (Life Expectancy)": "max"})\
            .withColumnRenamed("avg(Happiness Score)", "avg_happiness_score")\
            .withColumn("avg_happiness_score", col("avg_happiness_score").cast(DecimalType(34, 4)))
        return tdf
