from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, trim,unix_timestamp,col
from pyspark.sql.types import TimestampType , DoubleType

#Function to create Session
def create_session():
    return SparkSession.builder.appName('name').getOrCreate()

#Function to create Data frame
def create_df(sc,data,schema):
    return sc.createDataFrame(data,schema)


#Function to Formate the date & time
def format_timestamp(df,colName1):
    return df.withColumn(colName1, from_unixtime(df[colName1] / 1000).cast("timestamp"))



#Function to Change Data type
def format_type(df,colName1):
    return df.withColumn(colName1, to_date(df[colName1]))


#Function to trim the Space
def trim_str(df,ColName2):
    return df.withColumn(ColName2, trim(df[ColName2]))


#Function to replace null value
def fill_na(df,colName):
    return df.na.fill("", subset=[colName])


#question 2

#Function to rename column
def rename_column(df,existingName,newNam): #changet
    return df.withColumnRenamed(existingName,newNam)


#Function to convert to unix
def format_unix(df,colName1):
    timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    return df.withColumn('timestamp_format', unix_timestamp(df[colName1], timestamp_format).cast(DoubleType()))


#joining Table
def join_table(df,df2,colName,colName1):
    return df.join(df2,df[colName]==df2[colName1],"outer")