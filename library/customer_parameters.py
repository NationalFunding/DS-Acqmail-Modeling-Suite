# ============================================================
# MODEL CONFIGURATION 
# ============================================================

import os

# ------------------------------------------------------------
# TYPE OF PROBLEM
# ------------------------------------------------------------
# Accepted values:
#   'binary'     → classification
#   'regression' → regression
type_of_model = 'binary'


# # ------------------------------------------------------------
# # INPUT DATA
# # ------------------------------------------------------------
# # In Python, you should LOAD the data yourself and pass a DataFrame.
# # This is kept only for logging / metadata consistency.
# input_table = 'nf_dev_workarea.mk_df_nonppp_v7_resp_modeling'


# ------------------------------------------------------------
# COLUMNS TO EXCLUDE FROM FEATURE ENGINEERING
# ------------------------------------------------------------
exclude_from_feature_engineering_cols = [
    'duns', 'mail_date', 'load_year_prior', 'load_month_prior',
    'campaignid', 'accountnum', 'r', 'load_year', 'load_month',
    'flg_resp', 'flg_qual', 'flg_sub', 'flg_appr', 'flg_fund',
    'flg_NFappr', 'flg_NFfund', 'fund_amt', 'NFfund_amt',
    'fund_margin', 'NFfund_margin', 'ucc_filing_date'
]


# ------------------------------------------------------------
# MAILED VARIABLES
# ------------------------------------------------------------
mailed_vars = [
    'cadence_pattern','timesmailed','months_since_first',
    'months_since_last','flg_mailed',
    'cadence_m1','cadence_m2','cadence_m3',
    'cadence_m4','cadence_m5','cadence_m6',
    'mailed_6mo','mailed_12mo','mailed_24mo',
    'mailed_36mo','mailed_over_12mo',
    'flg_branddup_prior','branddup_3mo','branddup_6mo'
]


# ------------------------------------------------------------
# KEY IDENTIFIERS
# ------------------------------------------------------------
input_duns_col = "duns"        # unique identifier
weight = 'wt'                 # set to None if no weights are used


# ------------------------------------------------------------
# MODEL MODES
# ------------------------------------------------------------
if type_of_model == 'binary':
    mode = ['ctb', 'lgb', 'xgb']
elif type_of_model == 'regression':
    mode = ['lgb', 'xgb']
else:
    raise ValueError(
        f"Invalid type_of_model '{type_of_model}'. "
        "Accepted values: 'binary' or 'regression'"
    )


# ------------------------------------------------------------
# VARIABLES TO DROP FROM TRAINING
# ------------------------------------------------------------
drop_list = ['duns', 'load_month', 'load_year']


# ------------------------------------------------------------
# OUTPUT / ARTIFACT PATHS 
# ------------------------------------------------------------

BASE_ARTIFACT_DIR = os.getenv(
    "MODEL_ARTIFACT_DIR",
    "./model_artifacts"
)

os.makedirs(BASE_ARTIFACT_DIR, exist_ok=True)

customer_name = os.getenv("CUSTOMER_NAME", "default_customer")
model_name = os.getenv("MODEL_NAME", "automl_model")
model_version = os.getenv("MODEL_VERSION", "v1")


model_pickle_path = os.path.join(
    BASE_ARTIFACT_DIR,
    f"{customer_name}_{model_name}_{model_version}_{type_of_model}.pkl"
).replace(" ", "_")

print("model_pickle_path -->", model_pickle_path)


# ------------------------------------------------------------
# TRANSFORMATION MODE
# ------------------------------------------------------------
# Accepted values:
#   'Pandas_transform'
#   'Spark_transform'  (NOT SUPPORTED OUTSIDE DATABRICKS)
#   None
transformation_mode = None
