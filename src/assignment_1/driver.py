from PysparkRepo.src.assignment_1.utils import  *


Schema=["Product Name","Issue Date","Price","Brand","Country","Product number"]
dataStruct = [('Washing Machine',1648770933000,20000,'Samsung','India',1),
      ('Refrigerator',1648770999000,35000,' LG',None,2),
      ('Air Cooler',1648770948000,45000,'Voltas ',None,3)]
#df = sc.createDataFrame(dataStruct,Schema)

colName1="Issue Date"
colName="Country"
ColName2="Brand"

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
