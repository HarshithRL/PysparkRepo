import unittest
from pyspark.sql import SparkSession


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("unit_test").getOrCreate()

    def tearDown(self):
        self.spark.stop()
    def test_count_of_unique_location(self):

        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
