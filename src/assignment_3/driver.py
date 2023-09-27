from PysparkRepo.src.assignment_1.utils import  create_session ,create_df
from PysparkRepo.src.assignment_3.utils import * #import with names


data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]

#start the session
sc=create_session()

#create data frame
df=create_df(sc,data,columns)

colname3="amount"
colName2="country"
colname="product"

#Pivot The Table
pivotDF=pivot(df,colname,colName2,colname3)
pivotDF.show()
#unpivot the table
unpivot(colname,pivotDF).show()