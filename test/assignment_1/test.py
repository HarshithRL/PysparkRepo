import unittest

from pyspark.sql.types import *

from PysparkRepo.src.assignment_1.utils import *

class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("unit_test").getOrCreate()
        self.Schema = ["Product Name", "Issue Date", "Price", "Brand", "Country", "Product number"]
        self.dataStruct = [('Washing Machine', 1648770933000, 20000, 'Samsung', 'India', 1),
                      ('Refrigerator', 1648770999000, 35000, ' LG', None, 2),
                      ('Air Cooler', 1648770948000, 45000, 'Voltas ', None, 3)]
        self.schema_1 = StructType([
            StructField("SourceId", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("Language", StringType(), True),
            StructField("ModelNumber", IntegerType(), True),
            StructField("StartTime", StringType(), True),
            StructField("Product number", IntegerType(), True)
        ])

        # Define the data for dataStruct_1
        self.dataStruct_1 = [
            (150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', 1),
            (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', 2),
            (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', 3)
        ]
        self.expected_product = self.spark.createDataFrame(data=self.dataStruct, schema=self.Schema)
        self.expected_time = self.spark.createDataFrame(data=self.dataStruct_1, schema=self.schema_1)
    def tearDown(self):
        self.spark.stop()
    def test_format_timestamp(self):
        #testing format_timestamp function
        # Convert the Issue Date with the timestamp format
        self.schema = StructType([
            StructField("Product Name", StringType()),
            StructField("Issue Date", TimestampType()),
            StructField("Price", IntegerType()),
            StructField("Brand", StringType()),
            StructField("Country", StringType()),
            StructField("Product number", IntegerType())
        ])

        # Create a list of rows with the data
        self.data = [
            ("Washing Machine", "2022-04-01 05:25:33", 20000, "Samsung", "India", 1),
            ("Refrigerator", "2022-04-01 05:26:39", 35000, "LG", None, 2),
            ("Air Cooler", "2022-04-01 05:25:48", 45000, "Voltas", None, 3)
        ]

        # Create a DataFrame
        acatual_timestamp=self.spark.createDataFrame(data=self.data, schema=self.schema)
        colName1 = "Issue Date"
        self.expected_timestamp=format_timestamp(self.expected_time,colName1)

        self.assertEqual(sorted(acatual_timestamp), sorted(self.expected_timestamp))

    def test_format_type(self):
        # testing Format type function
        # Convert timestamp to date type
        schema = StructType([
            StructField("Product Name", StringType()),
            StructField("Issue Date", StringType()),
            StructField("Price", IntegerType()),
            StructField("Brand", StringType()),
            StructField("Country", StringType()),
            StructField("Product number", IntegerType())
        ])

        # Create a list of rows with the data
        data = [
            ("Washing Machine", "2022-04-01", 20000, "Samsung", "India", 1),
            ("Refrigerator", "2022-04-01", 35000, "LG", None, 2),
            ("Air Cooler", "2022-04-01", 45000, "Voltas", None, 3)
        ]

        # Create a DataFrame
        actual_df = self.spark.createDataFrame(data=data, schema=schema)

        # Apply the format_type function to convert the "Issue Date" column to date format
        self.expected_df = format_type(self.expected_timestamp, "Issue Date")

        self.assertEqual(sorted(actual_df.collect()),sorted(self.expected_df.collect()))

    def test_fill_na(self):
        # testing fill_na function
        colName = "Country"
        self.expected_df_result=fill_na(self.expected_df, colName)
        schema = StructType([
            StructField("Product Name", StringType()),
            StructField("Issue Date", DateType()),
            StructField("Price", IntegerType()),
            StructField("Brand", StringType()),
            StructField("Country", StringType()),
            StructField("Product number", IntegerType())
        ])

        # Create a list of rows with the data
        data = [
            ("Washing Machine", "2022-04-01", 20000, "Samsung", "India", 1),
            ("Refrigerator", "2022-04-01", 35000, "LG", "", 2),
            ("Air Cooler", "2022-04-01", 45000, "Voltas", "", 3)
        ]

        actual_result=self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_result.collect()), sorted(self.expected_df_result.collect()))

if __name__ == '__main__':
    unittest.main()
