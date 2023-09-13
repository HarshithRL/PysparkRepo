from PysparkRepo.src.assignment_1.utils import  *
df=create_df(sc,dataStruct,Schema)

#Convert the Issue Date with the timestamp format
df=format_date(df,colName1)
df.show()

#Convert timestamp to date type
df=format_type(df,colName1)
df.show()

#Remove the starting extra space in Brand column for LG and Voltas fields
df=trim_str(df,ColName2)
df.show()

#Replace null values with empty values in Country column
df=fill_na(df,colName)
df.show()

#stop Session
sc.stop()
