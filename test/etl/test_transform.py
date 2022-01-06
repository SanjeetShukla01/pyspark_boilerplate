"""
# test_transform.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:38 AM 12/31/2021 using PyCharm
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType, FloatType
# from src.etl.transform import Transform


class TransformTest(unittest.TestCase):
    def get_spark_session(self):
        self.spark = SparkSession.builder.appName("Preprocess Unit Test").getOrCreate()
        return self.spark

    def get_spark_df(self, spark, data, schema):
        df = spark.createDataFrame(data=data, schema=schema)
        return df

    def get_mock_input_df(self):
        """
           Get the input dataframe
           :return: data and the schema for the dataframe
           """
        data = [("Switzerland", "Western Europe", 1, 7.587, 0.03411, 1.39651, 1.34951, 0.94143, 0.66557, 0.41978, 0.29678, 2.51738),
                ("Iceland", "Western Europe", 2, 7.561, 0.04884, 1.30232, 1.40223, 0.94784, 0.62877, 0.14145, 0.4363, 2.70201)]
        schema = StructType([StructField("Country", StringType(), True),
                             StructField("Region", StringType(), True),
                             StructField("Happinss Rank", IntegerType(), True),
                             StructField("Happiness Score", DecimalType(), True),
                             StructField("Standard Error", FloatType(), True),
                             StructField("Economy(GDP per Capita)", DecimalType(), True),
                             StructField("Family", DecimalType(), True),
                             StructField("Health(Life Expectancy)", FloatType(), True),
                             StructField("Freedom", FloatType(), True),
                             StructField("Trust(Government Corruption)", FloatType(), True),
                             StructField("Generosity", FloatType(), True),
                             StructField("Dystopia Residual", DecimalType(), True),
                             ])
        return data, schema

    # def test_transform_should_return_transformed_data(self):
    #     transform = Transform
    #     spark = SparkSession.builder.appName("Preprocess Unit Test").getOrCreate()
    #     data, schema = self.get_mock_input_df()
    #     input_df = self.get_spark_df(spark, data, schema)
    #     input_df.show()
    #     actual_df = transform.transform_data(input_df)
    #     self.assertEqual(3, 3)

    def test_transform_data(self):
        self.assertEqual(3, 3)

    def test_another_test(self):
        self.assertTrue("PYTHON".isupper())


if __name__ == "__main__":
    unittest.main()
