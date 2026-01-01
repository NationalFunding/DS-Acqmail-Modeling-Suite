# Databricks notebook source
RUN_IN_PROD = False
RUN_DATE_OVERRIDE = '2025-01-01'
mth_title = 'Mar2025'
ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE = False

# COMMAND ----------

# use /dbfs since we are reading in with pandas
BUSINESS_NAME_LEMMATIZATIONS_FILE = "/dbfs/FileStore/tables/AMP_intake/lemmatized_names-1.parquet"
MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED = 180
INTAKE_FOLDER = "/dbfs/FileStore/tables/AMP_intake"
STAGING_FOLDER = "/dbfs/FileStore/tables/AMP_staging"
DEV_FOLDER = "/dbfs/FileStore/tables/AMP_dev"

# COMMAND ----------

# Counts for SBL selection
SBL_NF_ONLY_COUNT = 17500
SBL_QB_ONLY_COUNT = 17500

# COMMAND ----------

MLFLOW_KEYS = {'V7': {'RESP_nonppp_nfc1' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'], 
'RESP_nonppp_nfc2' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'], 
'RESP_ppp_nfc1' :['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'],
'RESP_ppp_nfc2' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'],
'SUB_nonppp_nfc1' : ['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_nonppp_nfc2' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_ppp_nfc1' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_ppp_nfc2' : ['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 

'RESP_nonppp_qbc1' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'], 
'RESP_nonppp_qbc2' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'], 
'RESP_ppp_qbc1' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'],
'RESP_ppp_qbc2' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'],
'SUB_nonppp_qbc1' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_nonppp_qbc2' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_ppp_qbc1' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_ppp_qbc2' : ['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'],
'RESP_nonppp_nfc1' :['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'], 
'RESP_nonppp_nfc2' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'], 
'RESP_ppp_nfc1' : ['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'],
'RESP_ppp_nfc2' :['77f8c83b935242fa817ca8bf85ae1b68', '20251119_014617', 'lgb'],
'SUB_nonppp_nfc1' : ['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_nonppp_nfc2' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_ppp_nfc1' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
'SUB_ppp_nfc2' :['f1cc15cfad0c4e0ea3aa00f1fa8b50b8', '20251119_031413', 'ctb'], 
}}

# COMMAND ----------

# Cutoffs
TIB_CUTOFFS = [0,1,6,float("inf")]
EMP_CUTOFFS = [0,2,5,float("inf")]
CCS_POINTS_CUTOFFS = [float("-inf"), 472, 521, float("inf")]
SALES_FUND_CUTOFFS = [0,275000,900000,float("inf")]
BA_CNT_CR_12M_CUTOFFS = [float("-inf"), 5, 17, float("inf")]
INQUIRY_DUNS_24M_CUTOFFS = [float("-inf"), 6 , 13, float("inf")]
SALES_APPR_CUTOFFS = [float("-inf"), 294710 , float("inf")]
STE_MKT_CROSSWALK = {
'Core': ['Sales_2-TIB_3-EMP_2'
,'Sales_3-TIB_1-EMP_2'
,'Sales_1-TIB_3-EMP_3'
,'Sales_2-TIB_2-EMP_2'
,'Sales_2-TIB_1-EMP_3'
,'Sales_1-TIB_2-EMP_3'
,'Sales_2-TIB_3-EMP_1'
,'Sales_3-TIB_1-EMP_1'
,'Sales_1-TIB_3-EMP_2'
,'Sales_2-TIB_2-EMP_1'
,'Sales_2-TIB_1-EMP_2'
,'Sales_1-TIB_2-EMP_2'
], 
'Prime': ['Sales_3-TIB_3-EMP_3'
,'Sales_3-TIB_2-EMP_3'
,'Sales_3-TIB_3-EMP_2'
,'Sales_2-TIB_3-EMP_3'
,'Sales_3-TIB_2-EMP_2'
,'Sales_3-TIB_1-EMP_3'
,'Sales_3-TIB_3-EMP_1'
,'Sales_2-TIB_2-EMP_3'
,'Sales_3-TIB_2-EMP_1'
],
'Select': ['Sales_1-TIB_1-EMP_3'
,'Sales_1-TIB_3-EMP_1'
,'Sales_2-TIB_1-EMP_1'
,'Sales_1-TIB_2-EMP_1'
,'Sales_1-TIB_1-EMP_2'
,'Sales_1-TIB_1-EMP_1'
]
}

# col_bins = {
# 'gc_employees' : EMP_CUTOFFS, 
# 'gc_sales' : SALES_CUTOFFS,
# 'TIB' : TIB_CUTOFFS
# }

# COMMAND ----------

MODEL_SCOREBIN_CUTOFFS = {
        'RESP_NONPPP': [0, 0.034114, 0.068916, 0.111961, 0.162962, 0.222099, 0.284987, 0.354618, 0.437950, 0.543222, 1],
        'RESP_PPP': [0, 0.034114, 0.068916, 0.111961, 0.162962, 0.222099, 0.284987, 0.354618, 0.437950, 0.543222, 1],
        'SUB_NONPPP':[0,0.186932, 0.266917, 0.325710, 0.377214, 0.426287, 0.479018, 0.536608, 0.607101, 0.705895, 1],
        'SUB_PPP':[0,0.186932, 0.266917, 0.325710, 0.377214, 0.426287, 0.479018, 0.536608, 0.607101, 0.705895, 1]
                        }

# COMMAND ----------

# SIC2 EXCLUSION LIST
sic2_exclusion_list = ['20','13','28','67','33','2','44','81','26','25','30','63','22','14','89','84','62','29','8','92','31','61','10','12','9','40','46','60','97','21','95','96','94','93','43', '91']