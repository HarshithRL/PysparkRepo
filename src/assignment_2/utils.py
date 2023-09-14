
from pyspark.sql.types import *
from pyspark.sql.functions import *


#1.Function to 	Select columns from Dataframe.
def select_column(df,colName):
    df_selected=df.select(df[colName])
    return df_selected
#2.Function to 	Add  column in the dataframe.
def add_column(df,colName):
    df_added_column = df.withColumn(colName, lit(''))
    return df_added_column

#3.	Function to  Change the value of  column.
def add_value_col(df,colName):
    df5 = df.withColumn('added_colname', col(colName) * 2)
    return df5

#4.Function to 	Change the data types
def change_data_type(df,colName,datatype):
    df4 = df.withColumn('New_salary', col(colName).cast(datatype))
    return df4

#5.Function to 	Derive new column from existing column
def add_new_column(df,colName,NewName):
    df=df.withColumn(NewName,col(colName))
    return df

#6.Function to Derive new column from salary column
def change_nested_name(df,colname,schema):
    df=df.withColumn(colname, col(colname).cast(schema))
    return df

#7.	Function to Filter the  column .
def filter_value(df,colName,colName1):
    max_value = df.agg({colName: "max"}).collect()[0][0]
    result = df.filter(col(colName) == max_value).select(colName1)
    return result

#8.	Function to List out distinct value column
def distinct_values(df,colName):
    df=df.select(colName).distinct()
    return df

#8. Function Drop the column
def drop_col(df,colName):
    df_new=df.drop(colName)
    return df_new