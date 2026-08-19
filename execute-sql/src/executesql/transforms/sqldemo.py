#################################################################
# Filename: dim_account_transforms.py
# Author: Prachin Soparkar
# Date: Jun 15, 2025
# Usage: Used by tran_gold_edw_dim_account notebook
#          
# Version | Author              | Date        | Description
# 1.0     | Prachin Soparkar    | 2025-08-05  | FIN-88: Initial
# 2.0     | Prachin Soparkar    | 2026-02-05  | Next-381 Add column subaccount
#################################################################
import sys
import os
from pathlib import Path
filePath = Path(__file__)
utilsPath = filePath.parent.parent.absolute()
sys.path.append(os.path.abspath(utilsPath))
from utils.sql_helper import *
 
class sqldemo(sql_helper):
  def __init__(self):
    super().__init__()
    spark = self.setSpark()    
    if self.getEnvName() == 'dv':
      spark.conf.set("spark.databricks.remoteFiltering.blockSelfJoins", "false")    

  def getDataFrame(self):
    envName = self.getEnvName()
    spark = self.getSpark()

    df = spark.sql(f"""SELECT
          accountid as accountNumber
          , objectAccount as objectAccountNumber
          , coalesce(nullif(CategoryCodeGL027, ''), '00') as profitandLossGroupingCode
          , am.subsidiary as subaccount
          , current_timestamp() as createDateTime
          , current_timestamp() as updateDateTime
      FROM {envName}_silver.jde.vwAccountMaster AS am
       """)
    return df