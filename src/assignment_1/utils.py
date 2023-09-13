from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, trim
sc=SparkSession.builder.appName('name').getOrCreate()

def create_session():
    return SparkSession.builder.appName('name').getOrCreate()
def create_df(sc,data,schema):
    return sc.createDataFrame(data,schema)

def format_date(df,colName1):
    return df.withColumn(colName1, from_unixtime(df[colName1] / 1000).cast("timestamp"))
def format_type(df,colName1):
    return df.withColumn(colName1, to_date(df[colName1]))
def trim_str(df,ColName2):
    return df.withColumn(ColName2, trim(df[ColName2]))
def fill_na(df,colName):
    return df.na.fill("", subset=[colName])


