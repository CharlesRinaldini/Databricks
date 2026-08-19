#################################################################
# Filename: shared_transforms.py
# Author: Charles Rinaldini
# Date: Jun 6, 2025
# Usage: Used by all notebooks
#          
# Version | Author              | Date        | Description
# 1.0     | Charles Rinaldini   | 2025-06-06  | Initial
#################################################################

# from databricks.sdk.runtime import *
from pyspark.sql import SparkSession 
import os

spark = SparkSession.builder.appName("edw").getOrCreate()

#imports for sql
from pyspark.sql.types import *
from pyspark.sql.functions import *

def mergeSCD1Changes(self, i_sourceDF, i_targetTable, i_rankCols):
    joinConditions = ""
    for rankCol in i_rankCols:
      joinConditions += f"s.{rankCol} = t.{rankCol} and "
    joinConditions = joinConditions[:-5]
    i_sourceDF.createOrReplaceTempView("src")
    spark.sql(f"""
      merge into {i_targetTable} as t
      using src as s
        on {joinConditions}
      when matched and s.syncOperation != 'D' then
        update set *
      when matched and s.syncOperation = 'D' then
        update set t.syncOperation = 'D'
      when not matched then
        insert *    
    """)
    return

def rankHashAndDeduplicateTable(self, i_df, i_rankCols, i_orderCol):
    #this assumes i_df is a chain or record changes over time and not just current record i.e. for a full reload
    df = i_df.withColumn("rn", rank().over(Window.partitionBy(*i_rankCols).orderBy(col(i_orderCol).desc())))
    df = df.filter(col("rn") == 1).drop("rn")
    otherCols = df.columns
    otherCols.remove(i_orderCol)
    otherCols.remove("startDate")
    o_df = df.withColumn("type2Hash", sha2(concat_ws("|", *otherCols), 256))
    return o_df
  
def compareSCD2Tables(self, i_sourceDF, i_targetTable, i_rankCols):
    joinConditions = ""
    for rankCol in i_rankCols:
      joinConditions += f"s.{rankCol} = t.{rankCol} and "
    joinConditions = joinConditions[:-5]

    i_sourceDF.createOrReplaceTempView("src")
    o_df = spark.sql(f"""
          select * 
          from src as s
          left join {i_targetTable} as t
          on {joinConditions}
          and s.type2Hash != t.type2Hash
          where t.type2Hash is null
          """)
    return o_df

def mergeSCD2Changes(self, i_sourceDF, i_targetTable, i_rankCols):
    joinConditions = ""
    for rankCol in i_rankCols:
      joinConditions += f"s.{rankCol} = t.{rankCol} and "
    joinConditions = joinConditions[:-5]
    i_sourceDF.createOrReplaceTempView("src")

    i_sourceDF.withColumn("endDate", lit(None)).filter(col("syncOperation") != 'D').write.insertInto(i_targetTable, overwrite = False)

    spark.sql(f"""
      merge into {i_targetTable} as t
      using src as s
        on {joinConditions}
        and t.endDate is null
      when matched and s.syncOperation != 'D' and s.type2Hash != t.type2Hash then
        update set t.endDate = s.startDate - interval 1 second
      when matched and s.syncOperation = 'D' then
        update set t.endDate = s.startDate
      when not matched by target then
        insert *    
    """)
    return
    
