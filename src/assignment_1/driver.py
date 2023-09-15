from PysparkRepo.src.assignment_1.utils import  *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


Schema=["Product Name","Issue Date","Price","Brand","Country","Product number"]
dataStruct = [('Washing Machine',1648770933000,20000,'Samsung','India',1),
      ('Refrigerator',1648770999000,35000,' LG',None,2),
      ('Air Cooler',1648770948000,45000,'Voltas ',None,3)]
schema_1 = StructType([
    StructField("SourceId", IntegerType(), True),
    StructField("TransactionNumber", IntegerType(), True),
    StructField("Language", StringType(), True),
    StructField("ModelNumber", IntegerType(), True),
    StructField("StartTime", StringType(), True),
    StructField("Product number", IntegerType(), True)
])

# Define the data for dataStruct_1
dataStruct_1 = [
    (150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', 1),
    (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', 2),
    (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', 3)
]


colName1="Issue Date"
colName="Country"
ColName2="Brand"

#start Session
sc=create_session()

"""Question 1"""
#create Data Frame
df_product_1=create_df(sc,dataStruct,Schema)


#Convert the Issue Date with the timestamp format
df_formate_timestamp_product_1=format_timestamp(df_product_1,colName1)
df_formate_timestamp_product_1.show()

#Convert timestamp to date type
df_formate_date_product_1=format_type(df_formate_timestamp_product_1,colName1)
df_formate_date_product_1.show()

#Remove the starting extra space in Brand column for LG and Voltas fields
df_space_trim_product_1=trim_str(df_formate_date_product_1,ColName2)
df_space_trim_product_1.show()

#Replace null values with empty values in Country column
df_replaced_null_product_1=fill_na(df_space_trim_product_1,colName)
df_replaced_null_product_1.show()

"""Question 2"""

#create Data Frame
df_product_2=create_df(sc,dataStruct_1,schema_1)

df_renamed=rename_column(df_product_2,'SourceId','source_id')

df_renamed=rename_column(df_renamed,'TransactionNumber','transaction_number')

#formate from timestamp to unix
df_format_unix=format_unix(df_renamed,'StartTime')
df_format_unix.show()

#Join the table
joined_table=join_table(df_replaced_null_product_1,df_product_2,"Product number","Product number")
joined_table.show()

#stop Session
sc.stop()
