from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, trim


#Function to create Session
def create_session():
    return SparkSession.builder.appName('name').getOrCreate()

#function to create Data frame
def create_df(sc,data,schema):
    return sc.createDataFrame(data,schema)

#function to Formate the date & time
def format_date(df,colName1):
    return df.withColumn(colName1, from_unixtime(df[colName1] / 1000).cast("timestamp"))

#function to Change Data type
def format_type(df,colName1):
    return df.withColumn(colName1, to_date(df[colName1]))

#function to trim the Space
def trim_str(df,ColName2):
    return df.withColumn(ColName2, trim(df[ColName2]))

#function to replace null value
def fill_na(df,colName):
    return df.na.fill("", subset=[colName])


