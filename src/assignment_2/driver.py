from PysparkRepo.src.assignment_1.utils import  create_session ,create_df
from PysparkRepo.src.assignment_2.utils import *

#Data & Schema for DataFrame
schema=StructType([
       StructField('name',StructType([
            StructField('firstname',StringType(),True),
            StructField('middlename',StringType(),True),
            StructField('lastname',StringType(),True)
       ])),
       StructField('dob',StringType(),True),
       StructField('gender',StringType(),True),
       StructField('salary',LongType(),True)
])
dataDF = [(('James','','Smith'),'03011998','M',3000),
  (('Michael','Rose',''),'10111998','M',20000),
  (('Robert','','Williams'),'02012000','M',3000),
  (('Maria','Anne','Jones'),'03011998','F',1100),
  (('Jen','Mary','Brown'),'04101998','F',10000)
]
#start the session
sc=create_session()

#create data frame
df=create_df(sc,dataDF,schema)

# 1.Select firstname, lastname and salary from Dataframe.
df_selected,df_selected1,df_selected2=select_column(df,'name.firstname'),select_column(df,'name.lastname'),select_column(df,'salary')
df_selected.show()
# df.select(df[colName]).show()

# 2.Add Country, department, and age column in the dataframe.
df=add_column(df,'Country')
df=add_column(df,'department')
df=add_column(df,'age')

# 3.Change the value of salary column.
add_value_col(df,'salary').show()

# 4.Change the data types of DOB and salary to String
df=change_data_type(df, 'salary', 'string')
df=change_data_type(df, 'dob', 'string')
df.show()

# 5.Derive new column from salary column.
df=add_new_column(df,'salary','salary_new')

# 6.Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
schema2 = StructType([
    StructField("firstposition",StringType()),
    StructField("secondposition",StringType()),
    StructField("lastposition",StringType())])
change_nested_name(df,'name',schema2).show()

# 7.Filter the name column whose salary in maximum.
filter_value(df,'salary','name').show()

# 8.Drop the department and age column.
df=drop_col(df,'department')
df=drop_col(df,'age')
df.show()

# 9.List out distinct value of dob and salary
distinct_values(df,'salary').show()

#Stop Session
sc.stop()




