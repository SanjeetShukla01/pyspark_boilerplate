"""
# test_transform.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 10:38 AM 12/31/2021 using PyCharm
"""
import unittest
from pyspark.sql import SparkSession
# from src.utils import utils
# from src.etl import transform


class TransformTest(unittest.TestCase):

    def test_transform_should_return_transformed_data(self):
        # spark = SparkSession.builder.appName("app_name").enableHiveSupport().getOrCreate()
        # test_utils = utils.Utils(self)
        # df = test_utils.read_csv("test/resources/data/mockdata.csv")
        # df.show()
        self.assertEqual(3, 3)



    def test_transform_data(self):
        self.assertEqual(3, 3)

    def test_another_test(self):
        self.assertTrue("PYTHON".isupper())


if __name__ == "__main__":
    unittest.main
