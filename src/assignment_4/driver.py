from PysparkRepo.src.assignment_4.utils import *
from PysparkRepo.src.assignment_1.utils import  *

employee_schema = StructType([StructField("employee_name", StringType(), True),
                              StructField("department", StringType(), True),
                              StructField("salary", IntegerType(), True)
                              ])
employee_data = [("James", "Sales", 3000),
                 ("Michael", "Sales", 4600),
                 ("Robert", "Sales", 4100),
                 ("Maria", "Finance", 3000),
                 ("Raman", "Finance", 3000),
                 ("Scott", "Finance", 3300),
                 ("Jen", "Finance", 3900),
                 ("Jeff", "Marketing", 3000),
                 ("Kumar", "Marketing", 2000)]

#start the session
spark = create_session()

#create the Data Frame
employee_df = create_df(spark,employee_data,employee_schema)
employee_df.show()

#2.	Select first row from each department group
dept_df = dept_grp(employee_df)
dept_df.show()

#3.	Create a Dataframe from Row and List of tuples.
new_df = row_data(spark)
new_df.show()

#5.	Retrieve Employees who earns the highest salary
highest_sal = highest_salary(employee_df)
highest_sal.show()

#6.	Select the highest, lowest, average, and total salary for each department group.
lowest_df = low_salary(employee_df)
lowest_df .show()

average_df = avg_salary(employee_df)
average_df.show()

total_df = total_salary(employee_df)
total_df.show()