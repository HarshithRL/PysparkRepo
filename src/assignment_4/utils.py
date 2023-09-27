from pyspark.sql import  Window
from pyspark.sql.types import *
from pyspark.sql.functions import *




def dept_grp(employee_df):
    partitioned_dept = Window.partitionBy("department").orderBy("employee_name")
    row_number_added = employee_df.withColumn("row_number", row_number().over(partitioned_dept))
    first_row = row_number_added.filter(row_number_added.row_number == 1).drop("row_number")
    return first_row


def row_data(spark):
    schema_emp = StructType([StructField("name", StringType(), True),
                             StructField("age", IntegerType(), True),
                             StructField("Job", StringType(), True)
                             ])
    row = ("Anil", 25, "IT")
    employee_data = [("Guna", 26, "Admin"),
                     ("Mani", 27, "HR")]

    added_data = [row] + employee_data

    new_df = spark.createDataFrame(data=added_data, schema=schema_emp)
    return new_df


def highest_salary(employee_df):
    highest_salary = Window.partitionBy("department").orderBy(col("salary").desc())
    row_number_added = employee_df.withColumn("row_number", row_number().over(highest_salary))
    filtered_df = row_number_added.filter(col("row_number") == 1).drop("row_number")
    return filtered_df


def multi_action(employee_df):#chanche
    # hightest salary
    high_salary = Window.partitionBy("department").orderBy(col("salary").desc())
    row_number_add1 = employee_df.withColumn("row_number", row_number().over(high_salary))
    high_df = row_number_add1.filter(col("row_number") == 1).drop("row_number")

    # lowest salary
    low_salary = Window.partitionBy("department").orderBy(col("salary").asc())
    row_number_add2 = employee_df.withColumn("row_number", row_number().over(low_salary))
    low_df = row_number_add2.filter(col("row_number") == 1).drop("row_number")
    low_df.show()
    # average salary
    avg_salary = employee_df.groupBy("department").agg(avg("salary"))
    avg_salary.show()
    # total salary for each department
    tot_salary = employee_df.groupBy("department").agg(sum("salary"))
    tot_salary.show()
    return high_df