"""
# test_transform.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:38 AM 12/31/2021 using PyCharm
"""
import unittest
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType, FloatType
import test.utils.test_utils
from etl.data_jobs.transform import Transform


class TransformTest(unittest.TestCase):

    def get_mock_input_df(self):
        """
           Get the input dataframe
           :return: data and the schema for the dataframe
           """
        data = [("Switzerland", "Western Europe", 1, 7.587, 0.03411, 1.39651, 1.34951, 0.94143, 0.66557, 0.41978, 0.29678, 2.51738),
                ("Iceland", "Western Europe", 2, 7.561, 0.04884, 1.30232, 1.40223, 0.94784, 0.62877, 0.14145, 0.4363, 2.70201)]
        schema = StructType([StructField("Country", StringType(), True),
                             StructField("Region", StringType(), True),
                             StructField("Happiness Rank", IntegerType(), True),
                             StructField("Happiness Score", FloatType(), True),
                             StructField("Standard Error", FloatType(), True),
                             StructField("Economy(GDP per Capita)", FloatType(), True),
                             StructField("Family", FloatType(), True),
                             StructField("Health (Life Expectancy)", FloatType(), True),
                             StructField("Freedom", FloatType(), True),
                             StructField("Trust(Government Corruption)", FloatType(), True),
                             StructField("Generosity", FloatType(), True),
                             StructField("Dystopia Residual", FloatType(), True),
                             ])
        return data, schema

    def test_transform_data(self):
        self.assertEqual(3, 3)

    def test_another_test(self):
        self.assertTrue("PYTHON".isupper())

    def test_transform_df(self):
        tester = test.utils.test_utils.TestUtils()
        spark = tester.get_spark_session()
        data, schema = self.get_mock_input_df()
        input_df = tester.get_spark_df(spark, data, schema)

        transformer = Transform(spark)
        transformed_df = transformer.transform_data(input_df)

        data, schema = self.get_mock_input_df()
        expected_df = tester.get_spark_df(spark, data, schema)
        expected_df = expected_df.filter(col("Happiness Rank") < 20) \
            .groupBy("Region").agg({"Happiness Score": "avg", "Health (Life Expectancy)": "max"}) \
            .withColumnRenamed("avg(Happiness Score)", "avg_happiness_score") \
            .withColumn("avg_happiness_score", col("avg_happiness_score").cast(DecimalType(34, 4)))
        assert (tester.compare_data_frames(transformed_df, expected_df))



if __name__ == "__main__":
    unittest.main()
