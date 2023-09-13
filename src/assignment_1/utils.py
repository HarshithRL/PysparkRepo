from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, trim
sc=SparkSession.builder.appName('name').getOrCreate()

Schema=["Product Name","Issue Date","Price","Brand","Country","Product number"]
dataStruct = [('Washing Machine',1648770933000,20000,'Samsung','India',1),
      ('Refrigerator',1648770999000,35000,' LG',None,2),
      ('Air Cooler',1648770948000,45000,'Voltas ',None,3)]
#df = sc.createDataFrame(dataStruct,Schema)

colName1="Issue Date"
colName="Country"
ColName2="Brand"


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


