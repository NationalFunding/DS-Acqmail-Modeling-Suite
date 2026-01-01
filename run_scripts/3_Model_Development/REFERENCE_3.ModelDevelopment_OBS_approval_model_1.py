# Databricks notebook source
save_model_artifacts = False

# COMMAND ----------


modeling_tbl_name_ppp = 'nf_dev_workarea.mk_df_ppp_v7_resp_modeling_update'
modeling_tbl_name_nonppp = 'nf_dev_workarea.mk_df_nonppp_v7_resp_modeling_update'
holdout_tbl_name_ppp = 'nf_dev_workarea.mk_df_ppp_v7_resp_holdout_update'
holdout_tbl_name_nonppp = 'nf_dev_workarea.mk_df_nonppp_v7_resp_holdout_update'
oot_tbl_name_ppp = 'nf_dev_workarea.mk_df_ppp_oot_v7_data_collection_resp_bigjoin_with_ref'
oot_tbl_name_nonppp = 'nf_dev_workarea.mk_df_nonppp_oot_v7_data_collection_resp_bigjoin_with_ref'



# COMMAND ----------

from pyspark.sql import functions as F
modeling_tbl_ppp_df = spark.table(modeling_tbl_name_ppp)
modeling_tbl_nonppp_df = spark.table(modeling_tbl_name_nonppp)
holdout_tbl_ppp_df = spark.table(holdout_tbl_name_ppp)
holdout_tbl_nonppp_df = spark.table(holdout_tbl_name_nonppp)
oot_tbl_ppp_df = spark.table(oot_tbl_name_ppp)
oot_tbl_nonppp_df = spark.table(oot_tbl_name_nonppp)


########### Adding 'flg_ppp'
modeling_tbl_ppp_df = modeling_tbl_ppp_df.withColumn("flg_ppp", F.lit(1))
modeling_tbl_nonppp_df = modeling_tbl_nonppp_df.withColumn("flg_ppp", F.lit(0))
holdout_tbl_ppp_df = holdout_tbl_ppp_df.withColumn("flg_ppp", F.lit(1))
holdout_tbl_nonppp_df = holdout_tbl_nonppp_df.withColumn("flg_ppp", F.lit(0))
oot_tbl_ppp_df = oot_tbl_ppp_df.withColumn("flg_ppp", F.lit(1))
oot_tbl_nonppp_df = oot_tbl_nonppp_df.withColumn("flg_ppp", F.lit(0))

ppp_cols = set(modeling_tbl_ppp_df.columns)
nonppp_cols = set(modeling_tbl_nonppp_df.columns)
missing_in_nonppp = list(ppp_cols - nonppp_cols)

from pyspark.sql.functions import lit

for c in missing_in_nonppp:
    modeling_tbl_nonppp_df = modeling_tbl_nonppp_df.withColumn(c, lit(None))
    holdout_tbl_nonppp_df = holdout_tbl_nonppp_df.withColumn(c, lit(None))
    oot_tbl_nonppp_df = oot_tbl_nonppp_df.withColumn(c, lit(None))





# Joining together
modeling_tbl_spark = modeling_tbl_ppp_df.unionByName(modeling_tbl_nonppp_df)
holdout_tbl_spark = holdout_tbl_ppp_df.unionByName(holdout_tbl_nonppp_df)
oot_tbl_spark = oot_tbl_ppp_df.unionByName(oot_tbl_nonppp_df)

# Filtered versions of each table
modeling_tbl_filt = modeling_tbl_spark.filter("flg_sub = 1")
holdout_tbl_filt = holdout_tbl_spark.filter("flg_sub = 1")
oot_tbl_filt = oot_tbl_spark.filter("flg_sub = 1")

#Adding in OBS Approval
nfp = (
    spark.table("nationalfunding_sf_share.load_as_df_campaign_performance_nf")
         .filter(F.col("RUN_DATE") == "20251201")
         .select("accountnum", "internal_approved_amount")
)

qbp = (
    spark.table("nationalfunding_sf_share.load_as_df_campaign_performance_qb")
         .filter(F.col("RUN_DATE") == "20251201")
         .select("accountnum", "internal_approved_amount")
)

modeling_tbl_filt = (
    modeling_tbl_filt
        .join(nfp.alias("nfp"), on="accountnum", how="left")
        .join(qbp.alias("qbp"), on="accountnum", how="left")
).withColumn(
    "flg_appr_obs",
    F.when(F.col("nfp.internal_approved_amount") > 0, 1)
     .when(F.col("qbp.internal_approved_amount") > 0, 1)
     .otherwise(0)
)
modeling_tbl_filt = modeling_tbl_filt.drop('internal_approved_amount')

holdout_tbl_filt = (
    holdout_tbl_filt
        .join(nfp.alias("nfp"), on="accountnum", how="left")
        .join(qbp.alias("qbp"), on="accountnum", how="left")
).withColumn(
    "flg_appr_obs",
    F.when(F.col("nfp.internal_approved_amount") > 0, 1)
     .when(F.col("qbp.internal_approved_amount") > 0, 1)
     .otherwise(0)
)
holdout_tbl_filt = holdout_tbl_filt.drop('internal_approved_amount')

oot_tbl_filt = (
    oot_tbl_filt
        .join(nfp.alias("nfp"), on="accountnum", how="left")
        .join(qbp.alias("qbp"), on="accountnum", how="left")
).withColumn(
    "flg_appr_obs",
    F.when(F.col("nfp.internal_approved_amount") > 0, 1)
     .when(F.col("qbp.internal_approved_amount") > 0, 1)
     .otherwise(0)
)
oot_tbl_filt = oot_tbl_filt.drop('internal_approved_amount')



# Register filtered temp views
modeling_tbl_filt.createOrReplaceTempView("modeling_tbl_temp")
holdout_tbl_filt.createOrReplaceTempView("holdout_tbl_temp")
oot_tbl_filt.createOrReplaceTempView("oot_tbl_temp")

# Counts and value checks
print(f"modeling_tbl_filt row count: {modeling_tbl_filt.count()}")
print(f"modeling_tbl_filt column count: {len(modeling_tbl_filt.columns)}")
modeling_tbl_filt.groupBy('flg_appr_obs').count().show()

print(f"holdout_tbl_filt row count: {holdout_tbl_filt.count()}")
holdout_tbl_filt.groupBy('flg_appr_obs').count().show()

print(f"oot_tbl_filt row count: {oot_tbl_filt.count()}")
oot_tbl_filt.groupBy('flg_appr_obs').count().show()


# modeling_tbl_spark.createOrReplaceTempView("modeling_tbl_temp")
# holdout_tbl_spark.createOrReplaceTempView("holdout_tbl_temp")

# oot_tbl_spark.createOrReplaceTempView("oot_tbl_temp")

# print(f"modeling_tbl_spark row count: {modeling_tbl_spark.count()}")
# print(f"modeling_tbl_spark column count: {len(modeling_tbl_spark.columns)}")

# # Value counts for a column
# modeling_tbl_spark.groupBy('flg_resp').count().show()

# # Repeat for other tables
# print(f"holdout_tbl_spark row count: {holdout_tbl_spark.count()}")
# holdout_tbl_spark.groupBy('flg_resp').count().show()

# print(f"oot_tbl_spark row count: {oot_tbl_spark.count()}")
# oot_tbl_spark.groupBy('flg_resp').count().show()

# COMMAND ----------

modeling_tbl_name = "modeling_tbl_temp"
holdout_tbl_name = "holdout_tbl_temp"
oot_tbl_name = "oot_tbl_temp"

# COMMAND ----------

modeling_tbl_filt.columns# = modeling_tbl_filt.drop('internal_approved_amount')

# COMMAND ----------

from hyperopt import hp
# Custom hyperparameter spaces to inject
custom_catboost_hyper  = {
    "n_estimators": hp.quniform("n_estimators", 100, 1000, 50),  # alias: iterations
    "max_depth": hp.quniform("max_depth", 3, 5, 1),
    "reg_lambda": hp.uniform("reg_lambda", 1, 3),  # alias: l2_leaf_reg
    "learning_rate": hp.uniform("learning_rate", 0.001, 0.05),
    "bagging_temperature": hp.quniform("bagging_temperature", 0.1, 1.0, 0.2),
    "scale_pos_weight": hp.quniform("scale_pos_weight", 0.01, 1.0, 0.2),
    "colsample_bylevel": hp.quniform("colsample_bylevel", 0.3, 0.9, 0.2),
    "subsample": hp.quniform("subsample", 0.2, 0.9, 0.2),
    "min_child_samples": hp.quniform("min_child_samples", 100, 500, 100)
}

custom_lgbm_hyper  = {
    "n_estimators": hp.quniform("n_estimators", 100, 1000, 50),  # alias: iterations
    "max_depth": hp.quniform("max_depth", 3, 5, 1),
    "reg_alpha": hp.uniform("reg_alpha", 1, 3),
    "reg_lambda": hp.uniform("reg_lambda", 1, 3),  # alias: l2_leaf_reg
    "learning_rate": hp.uniform("learning_rate", 0.001, 0.05),
    "subsample": hp.quniform("subsample", 0.5, 0.9, 0.2),  # alias: bagging_fraction
    "pos_bagging_fraction": hp.quniform("pos_bagging_fraction", 0.1, 1.0, 0.2),  # for imbalanced data
    "neg_bagging_fraction": hp.quniform("neg_bagging_fraction", 0.1, 1.0, 0.2),  # use with pos_bagging_fraction
    "colsample_bytree": hp.quniform("colsample_bytree", 0.3, 0.9, 0.2),
    "min_child_samples": hp.quniform("min_child_samples", 100, 500, 100)  # alias: min_data_in_leaf
}

custom_xgb_hyper  = {
    "n_estimators": hp.quniform("n_estimators", 100, 1000, 50),
    "max_depth": hp.quniform("max_depth", 3, 5, 1),  # np.arange(3, 8, dtype=int)
    "reg_alpha": hp.uniform("reg_alpha", 1, 3),  # L1 regularization
    "reg_lambda": hp.uniform("reg_lambda", 1, 3),  # L2 regularization
    "gamma": hp.quniform("gamma", 0.0, 1.0, 0.001),  # min loss reduction for splits
    "learning_rate": hp.uniform("learning_rate", 0.001, 0.05),
    "scale_pos_weight": hp.quniform("scale_pos_weight", 0.01, 1.0, 0.2),  # class imbalance ratio
    "colsample_bylevel": hp.quniform("colsample_bylevel", 0.3, 0.9, 0.2),  # alias: colsample_bytree
    "subsample": hp.quniform("subsample", 0.2, 0.9, 0.2),  # random sample before growing trees
    "min_child_weight": hp.quniform("min_child_weight", 0.01, 0.1, 0.01)
}

# COMMAND ----------

model_name = 'appr_obs' # Name of the Model
model_version = 'v7' # Model version
metric = 'flg_appr_obs'
STAGING_FOLDER = f"dbfs:/FileStore/tables/AMP_staging/model_development/model_development/{model_name}/"
dbutils.fs.mkdirs(STAGING_FOLDER)

excluded_cols = [
 'nf_last_mail_date',
 'nf_first_mail_date',
 'nf_num_times_mailed',
 'nf_cadence_pattern',
 'qb_last_mail_date',
 'qb_first_mail_date',
 'qb_num_times_mailed',
 'qb_cadence_pattern',
 'sbl_last_mail_date',
 'sbl_first_mail_date',
 'sbl_num_times_mailed',
 'sbl_cadence_pattern',
 'last_mail_date',
 'first_mail_date',
 'cadence_pattern',
  'zip_code', #too granular
 'tot_emp_cnt',
 #'brand',
 'bus_strt_yr',
 'employees_at_locn_cnt',
 'ann_sls_us_dolr_amt',
 'ser_score', 
 'sic4_num', 
 'sic4_primary_cd', 
 'orig_gc_sales',
 'orig_gc_employees',
 'months_since_ucc_null'
#,'qtr_mail'
,'DateApproved'
,'SBAOfficeCode'
,'BorrowerName'
,'BorrowerAddress'
,'BorrowerCity'
,'BorrowerZip'
,'BorrowerState'
,'Processing'
,'LoanStatusDate'
,'FranchiseName'
,'NAICSCode'
,'ForgivenessDate',
'LoanStatus',
'Term',
'Veteran',
'SBAGuarantyPercentage',
'InitialApprovalAmount',
'CurrentApprovalAmount',
'UndisbursedAmount',
'ServicingLenderName',
'ServicingLenderAddress',
'ServicingLenderCity',
'ServicingLenderState',
'ServicingLenderZip',
'ServicingLenderLocationID',
'RuralUrbanIndicator',
'HubzoneIndicator',
'LMIIndicator',
'BusinessAgeDescription',
'ProjectCity',
'ProjectCountyName',
'ProjectState',
'ProjectZip',
'CD',
'JobsReported',
'Race',
'Ethnicity',
'UTILITIES_PROCEED',
'PAYROLL_PROCEED',
'MORTGAGE_INTEREST_PROCEED',
'RENT_PROCEED',
'REFINANCE_EIDL_PROCEED',
'HEALTH_CARE_PROCEED',
'DEBT_INTEREST_PROCEED',
'BusinessType',
'OriginatingLender',
'OriginatingLenderCity',
'OriginatingLenderState',
'OriginatingLenderLocationID',
'Gender',
'NonProfit',
'ForgivenessAmount',
'ProcessingMethod',

#,'distribution_tier'

# The below were commented out
# ,'months_since_first',
# 'months_since_last',
# 'cadence_m1',
# 'cadence_m2',
# 'cadence_m3',
# 'cadence_m4',
# 'cadence_m5',
# 'cadence_m6',
# 'timesmailed',
# 'flg_mailed',
# 'mailed_6mo',
# 'mailed_12mo',
# 'mailed_24mo',
# 'mailed_36mo',
# 'mailed_over_12mo',

#  'sic2_qtr',
'flg_branddup_prior',
 'branddup_3mo',
 'branddup_6mo',
# 'num_times_mailed',
#  'brandcode',

'custom_response',
'custom_funded',

'load_month_gc',
'load_month',
'mail_date'

#  ,'months_since_ucc'

 ]

# COMMAND ----------

base_df = spark.table(modeling_tbl_name).toPandas().sort_values(by=['accountnum'])
if 'ref_score' not in base_df.columns:
    base_df['ref_score'] = 0
#MK commented out the below:
# if 'flg_NFappr' not in base_df.columns:
#     base_df['flg_NFappr'] = 0

mdl_set = base_df.columns
final_vars =  [c for c in base_df.columns if c not in excluded_cols]
importance_vars = []
duns_date_cols = []

base_df.shape

# COMMAND ----------

flg_ags = False
type_balanced = "balanced"
flg_log = False
flg_woe = False
eda_fit_feature_selection_execution=True

mandatory_cols = []
mandatory_cols = [c for c in mandatory_cols if c in final_vars]

# COMMAND ----------

# MAGIC
# MAGIC %run "/Shared/RespPlus V7 Models/Model Development/00_Customer_Parameters"

# COMMAND ----------

# MAGIC %run "/Shared/RespPlus V7 Models/Model Development/01_Data_Preparation"

# COMMAND ----------

# MAGIC
# MAGIC %run "/Shared/RespPlus V7 Models/Model Development/library/nf_model_automl_binary"
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run "/Shared/RespPlus V7 Models/Model Development/library/nf_Performance_Summary"

# COMMAND ----------

[c for c in final_vars if 'ref_score' ==c]

# COMMAND ----------

# DBTITLE 1,Feature Engineering
cat_cols = [c for c in base_df.select_dtypes(include='object').columns]
cat_cols = [c for c in cat_cols if c != metric]
num_cols = [c for c in base_df.columns if c not in cat_cols and c != metric]
#eda_vars = list(final_vars + importance_vars + duns_date_cols + mandatory_cols + [metric, 'mail_date'])
eda_vars = list(final_vars + importance_vars + duns_date_cols + mandatory_cols)
df_fe, fe_woeDF, log_cols = modeling_data_preprocessing_table(dfPandas = base_df, 
                                                     label = metric, 
                                                     cat_cols = cat_cols, 
                                                     num_cols = num_cols,
                                                     var_cols = eda_vars, 
                                                     flg_ags_arg = flg_ags,
                                                     type_balanced_arg = type_balanced, 
                                                     flg_log_arg = flg_log, 
                                                     flg_woe_arg = flg_woe)

# COMMAND ----------

# DBTITLE 1,eda_fit_feature_selection
model = nf_model_automl_binary()


model.catboost_hyper = custom_catboost_hyper
model.lgbm_hyper = custom_lgbm_hyper
model.xgb_hyper = custom_xgb_hyper

# set model parameters
model.top_n_prnct = 0.90
model.missing_threshold = 0.75
model.correlation_threshold = 0.8
model.vif_threshold = 4
model.drop_list = drop_list
model.fixed_vars = mandatory_cols
model.categorical_vars = [] # categorical variables cannot be used with mode as 'xgb' 
model.stratify_vars = []  # list of columns to have stratify split of train-test 
model.max_features = 30  ## upper-limit/maximum number of columns (excluding fixed_vars) to shortlist in eda_fit_feature_selection
model.kfolds = 7  # (0 --> does not run k-fold ; higher the value more time it will take to run)
model.mode = mode
model.comp_against = {} 
model.exclusion_list = [c for c in ['duns', 'mail_date', 'load_year_prior', 'load_month_prior', 'campaignid', 'accountnum', 'r', 'load_year', 'load_month', 'flg_resp', 'flg_qual', 'flg_sub', 'flg_appr','flg_appr_obs', 'flg_fund', 'flg_NFappr', 'flg_NFfund', 'fund_amt', 'NFfund_amt', 'funded_margin', 'NFfund_margin', 'ucc_filing_date', 'ref_score'] if (c != metric) & (c in df_fe.columns)]  # list of comp_against vars not to include in model building, just to be considered for comparison purpose
#model.exclusion_list = [c for c in ['duns', 'mail_date', 'load_year_prior', 'load_month_prior', 'campaignid', 'accountnum', 'r', 'load_year', 'load_month', 'flg_resp', 'flg_qual', 'flg_sub', 'ucc_filing_date', 'ref_score'] if (c != metric) & (c in df_fe.columns)]  # list of comp_against vars not to include in model building, just to be considered for comparison purpose
model.test_size = 0.2 # df_val --> pandas dataframe
model.input_duns_col = "duns"
model.target = metric
model.param_dict = {
    'weight': weight,
    'n_iter': 7,  # number of model user wants to train as part of hyper-parameter tuning
    'n_jobs': -1  # number of cores user wants to use during model training. -1 means it will use all the available cores in driver node
}
model.imp_woe = fe_woeDF
model.log_cols = log_cols

model.eda_fit_feature_selection(df_fe, execution=eda_fit_feature_selection_execution)

# COMMAND ----------

# DBTITLE 1,Hyper-parameter tuning 
model.df_pandas = model.df_pandas.loc[:, ~model.df_pandas.columns.duplicated()]
model.X = [x for x in model.X if x in model.df_pandas.columns]
model.fit_multi_iter(model.df_pandas)



# COMMAND ----------

# DBTITLE 1,performance comparison chart
chosen_model = model.best_iter_comp(ctb_nth_iter = None, lgb_nth_iter = None, xgb_nth_iter = None, best_model_measure='auc')

# COMMAND ----------

# DBTITLE 1,performance comparison table
model.metric_comparison_report()

# COMMAND ----------

# DBTITLE 1,Choose the final model - to be selected by user
model.fit(final_model = chosen_model)

# COMMAND ----------

model.enable_report()

# COMMAND ----------

# DBTITLE 1,model report
model.generate_report2()

# COMMAND ----------

# DBTITLE 1,Lift and Gain charts
#model.generate_charts()


# COMMAND ----------


from sklearn.metrics import precision_recall_curve, f1_score, accuracy_score, precision_score, recall_score
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from IPython.display import display, HTML

def gen_evaluation_metrics_new(dff, score_col_name, metric, optimize_threshold=True, min_precision=None):
    dff[metric] = pd.to_numeric(dff[metric], errors='coerce')
    dff['bestmodel_score'] = dff[score_col_name]


    dff['model_pred'] = 0
    dff.loc[dff['bestmodel_score'] >= 0.5, 'model_pred'] = 1

    score_bins = np.arange(0,1.1,0.1)
    dff['bestmodel_scorebin'] = (pd.cut(dff['bestmodel_score'], bins=score_bins, right=False))
    dff['bestmodel_decile'] = (pd.qcut(dff['bestmodel_score'], q=10, duplicates='drop')).rank(method='dense', ascending=False)

    dff['large_fund'] = 0
    dff.loc[dff['fund_amt'] >= 25000, 'large_fund'] = 1


    # KDE plot for score distribution
    g = sns.FacetGrid(dff, hue=metric, palette=['red', 'green'])
    g.map(sns.kdeplot, "bestmodel_score")
    plt.title(f"Score distribution by {metric}")
    plt.show()


    # ---- Metrics ----
    acc = accuracy_score(dff[metric], dff['model_pred'])
    prec_val = precision_score(dff[metric], dff['model_pred'])
    rec_val = recall_score(dff[metric], dff['model_pred'])
    f1_val = f1_score(dff[metric], dff['model_pred'])
    roc_auc_val = roc_auc_score(dff[metric],  dff['bestmodel_score'])

    print(f"roc_auc: {roc_auc_val:.3f}")
    print(f"accuracy: {acc:.3f}")
    print(f"precision: {prec_val:.3f}")
    print(f"recall: {rec_val:.3f}")
    print(f"f1_score: {f1_val:.3f}")

     # ---- Capture rate calculation ----
    df_sorted = dff.sort_values(by='bestmodel_score', ascending=False).reset_index(drop=True)
    total_positives = df_sorted[metric].sum()

    # Top 10% and 20%
    top_10 = df_sorted.head(int(len(df_sorted) * 0.10))
    top_20 = df_sorted.head(int(len(df_sorted) * 0.20))

    capture_rate_10 = top_10[metric].sum() / total_positives if total_positives > 0 else np.nan
    capture_rate_20 = top_20[metric].sum() / total_positives if total_positives > 0 else np.nan

    print(f"capture_rate_10%: {capture_rate_10:.3f}")
    print(f"capture_rate_20%: {capture_rate_20:.3f}")

    for col in ['flg_resp', 'flg_sub', 'flg_appr', 'flg_fund', 'fund_amt']:
        dff[col] = dff[col].astype(int)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)  # auto-detect width
    pd.set_option('display.max_colwidth', None)  # show full cell content

   
    scorebin_df = gen_summary_tab(df=dff, var='bestmodel_scorebin', target_label=metric, sort_ascending=False, full=True)
    #displayHTML(scorebin_df)
    decile_df = gen_summary_tab(df=dff, var='bestmodel_decile', target_label=metric, sort_ascending=True, full=True)
    #displayHTML(decile_df)
    
    # Convert Interval columns to strings
    for col in scorebin_df.select_dtypes(include=['interval']).columns:
        scorebin_df[col] = scorebin_df[col].astype(str)

    for col in decile_df.select_dtypes(include=['interval']).columns:
        decile_df[col] = decile_df[col].astype(str)

    # Display HTML safely
    displayHTML(scorebin_df.to_html())
    displayHTML(decile_df.to_html())

# COMMAND ----------

# DBTITLE 1,Save the model artifacts for production
if save_model_artifacts:
    mlflow_info = model.log_this_iteration()
    model_name = model_name_flg(model_name, flg_ags, type_balanced, flg_log, flg_woe)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Modeling Set - new model

# COMMAND ----------

modeling_df = holdout_data_preprocessing_table(modeling_tbl_name, 
                                            model.X, 
                                            model.cols_catg,
                                            model.log_cols, 
                                            model.imp_woe,
                                            metric)

modeling_df_scored = model.score(modeling_df, 
                model = model.finalmodel, 
                model_vars = model.X, 
                cols_catg = model.cols_catg, 
                pdo = 40, 
                basepoints = 1130)
                
gen_evaluation_metrics_new(dff = modeling_df_scored, score_col_name='proba_bad', metric = metric)

# COMMAND ----------

final_model = model.finalmodel
preds= model.X

for c in modeling_df.columns:
  col_type = modeling_df[c].dtype
  print(c,col_type)

# COMMAND ----------

final_model = model.finalmodel
preds= model.X

for c in modeling_df.columns:
  col_type = modeling_df[c].dtype
  if col_type == 'object' or col_type.name == 'category':
    modeling_df[c] = modeling_df[c].astype('category')

explainer = shap.TreeExplainer(final_model)
shap_values = explainer.shap_values(modeling_df[preds])

vals= np.abs(shap_values).mean(0)


# #The below code will give the feature importance based on shapely
feature_importance = pd.DataFrame(list(zip(preds, vals)), columns=['col_name','feature_importance_vals'])

#The below code will give the summary plot
shap.summary_plot(shap_values, modeling_df[preds],plot_type='bar')


# COMMAND ----------

try:
    shap_val = shap_values[1]
    shap.summary_plot(shap_val,modeling_df[preds],max_display=35, plot_size=[20,9])
except:
    shap_val = shap_values
    shap.summary_plot(shap_val,modeling_df[preds],max_display=35, plot_size=[20,9])

# COMMAND ----------

# import matplotlib.pyplot as plt
# import shap

# # Plotting the distribution and target rate for each numeric variable
# num_plots = len(model.X)
# n_cols = 2  # Number of columns of subplots
# num_rows = (num_plots + n_cols - 1) // n_cols  # Calculate number of rows needed

# # Plot each feature's partial dependence
# feature_importance['abs_mean'] = feature_importance['feature_importance_vals'].abs().mean()
# feature_order = feature_importance.sort_values(['abs_mean'], ascending=False)['col_name']
# for i, colname in enumerate(feature_order):
#     row = i // n_cols
#     col = i % n_cols
#     cutoff = modeling_df[colname].quantile(0.95)

#     # Plot partial dependence for the current feature
#     shap.plots.partial_dependence(
#         colname, model.finalmodel.predict, modeling_df[model.X], ice=False,
#         model_expected_value=True, feature_expected_value=True, xmax=cutoff
#     )

# plt.tight_layout()
# plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Holdout - new model

# COMMAND ----------

# DBTITLE 1,Holdout Performance
holdout_df = holdout_data_preprocessing_table(holdout_tbl_name, 
                                            model.X, 
                                            model.cols_catg,
                                            model.log_cols, 
                                            model.imp_woe,
                                            metric)

holdout_df_scored = model.score(holdout_df, 
                model = model.finalmodel, 
                model_vars = model.X, 
                cols_catg = model.cols_catg, 
                pdo = 40, 
                basepoints = 1130)
                
gen_evaluation_metrics_new(dff = holdout_df_scored, score_col_name='proba_bad', metric = metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OOT - new model

# COMMAND ----------

oot_df = holdout_data_preprocessing_table(oot_tbl_name, 
                                            model.X, 
                                            model.cols_catg,
                                            model.log_cols, 
                                            model.imp_woe,
                                            metric
                                            )

oot_df_scored = model.score(oot_df, 
                model = model.finalmodel, 
                model_vars = model.X, 
                cols_catg = model.cols_catg, 
                pdo = 40, 
                basepoints = 1130)
gen_evaluation_metrics_new(dff = oot_df_scored, score_col_name='proba_bad',  metric = metric)


# COMMAND ----------

# MAGIC %md
# MAGIC ### OOT New Model NonPPP

# COMMAND ----------

oot_nonppp_df = holdout_data_preprocessing_table(oot_tbl_nonppp_df, 
                                            model.X, 
                                            model.cols_catg,
                                            model.log_cols, 
                                            model.imp_woe,
                                            metric,
                                            use_spark_table = True)

oot_nonppp_df_scored = model.score(oot_nonppp_df, 
                model = model.finalmodel, 
                model_vars = model.X, 
                cols_catg = model.cols_catg, 
                pdo = 40, 
                basepoints = 1130)
gen_evaluation_metrics_new(dff = oot_nonppp_df_scored, score_col_name='proba_bad',  metric = metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OOT New Model PPP

# COMMAND ----------

oot_ppp_df = holdout_data_preprocessing_table(oot_tbl_ppp_df, 
                                            model.X, 
                                            model.cols_catg,
                                            model.log_cols, 
                                            model.imp_woe,
                                            metric,
                                            use_spark_table = True)

oot_ppp_df_scored = model.score(oot_ppp_df, 
                model = model.finalmodel, 
                model_vars = model.X, 
                cols_catg = model.cols_catg, 
                pdo = 40, 
                basepoints = 1130)
gen_evaluation_metrics_new(dff = oot_ppp_df_scored, score_col_name='proba_bad',  metric = metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full modeling set - reference model

# COMMAND ----------

gen_evaluation_metrics_new(dff = modeling_df_scored[modeling_df_scored['ref_score'].notnull()], score_col_name='ref_score',  metric = metric)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Holdout set - reference model

# COMMAND ----------

gen_evaluation_metrics_new(dff = holdout_df_scored[holdout_df_scored['ref_score'].notnull()], score_col_name='ref_score', metric = metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OOT set - reference model

# COMMAND ----------

gen_evaluation_metrics_new(dff = oot_df_scored[oot_df_scored['ref_score'].notnull()], score_col_name='ref_score',  metric = metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OOT Reference Set NONPPP

# COMMAND ----------

gen_evaluation_metrics_new(dff = oot_nonppp_df_scored[oot_nonppp_df_scored['ref_score'].notnull()], score_col_name='ref_score',  metric = metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OOT Reference Set PPP

# COMMAND ----------

gen_evaluation_metrics_new(dff = oot_ppp_df_scored[oot_ppp_df_scored['ref_score'].notnull()], score_col_name='ref_score',  metric = metric)