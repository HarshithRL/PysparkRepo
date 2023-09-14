from pyspark.sql.functions import expr

"""Function for Pivot the Table"""
def pivot(df,colname,colName2,colname3):
    pivotDF = df.groupBy(colname).pivot(colName2).sum(colname3)
    return pivotDF


"""Function for unPivot the Table"""
def unpivot(colname,pivotDF):
    unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
    unPivotDF = pivotDF.select(colname, expr(unpivotExpr)).where("Total is not null")
    return unPivotDF

