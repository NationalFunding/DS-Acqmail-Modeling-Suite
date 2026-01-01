

from functools import reduce
from operator import add
from scipy.stats import ttest_ind, chi2_contingency
import pandas as pd
from library import nl_utils
#from sql_scripts import sql_query_OOT
import numpy as np
from pathlib import Path
from datetime import timedelta, date
from handlers import Session
from dateutil.relativedelta import relativedelta
from snowflake.connector.pandas_tools import write_pandas
from hyperopt import hp
import matplotlib.pyplot as plt
import shap

from sklearn.metrics import precision_recall_curve, f1_score, accuracy_score, precision_score, recall_score
import seaborn as sns
from IPython.display import display, HTML

from library import (
    customer_parameters,
    data_preparation,
    nf_model_automl_binary,
    nf_Performance_Summary,
    evaluation_metrics
)
from library.nf_model_automl_binary import nf_model_automl_binary


if __name__ == "__main__":
    # ---------------------- Metrics, Model name definition ----------------------------------------------------------
    # --------------------------------------------------------------------------------------------------------------
    version = 'V1'
    modeling_tbl = pd.read_csv('iris.data')
    model_type = 'iris_example' #Prescreen_resp
    metric = 'label' #flg_resp
    save_model_artifacts = False

    # -------------------- Reading in data --------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------
    session = Session(use_log=True)
    # session.log.info(f"Running 3.ModelDevelopment_{model_type}_{version}.py...")
    session.log.info(f"Running 3.ModelDevelopment_EXAMPLE.py...")
    mail_month = session.mail_dt.strftime('%Y%m')
    mail_month_sql = session.mail_dt.strftime('%Y%m%d')
    run_month_sql =   session.run_dt.strftime('%Y%m%d')
    modeling_tbl_name = "modeling_tbl_temp"

 

    # --------------------------------------- Preparing for feature engineering & model parameters ----------------------
    excluded_cols = []
    base_df = modeling_tbl #.sort_values(by=['accountnum'])
    mdl_set = base_df.columns
    final_vars =  [c for c in base_df.columns if c not in excluded_cols]
    importance_vars = []
    #duns_date_cols = []
    base_df.shape

    flg_ags = False
    type_balanced = "balanced"
    flg_log = False
    flg_woe = False
    eda_fit_feature_selection_execution=True

    mandatory_cols = []
    mandatory_cols = [c for c in mandatory_cols if c in final_vars]

       # ----------------------------- Hyperparameter tuning ----------------------------------------------------------
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

    # ------------------------------- Feature Engineering & Importance -------------------------------------------

    #Empty Dataframe
    newDF,woeDF = pd.DataFrame(), pd.DataFrame()
    #Extract Column Names
    cols = base_df.columns
    #Run WOE and IV on all the independent variables
   
    cat_cols = [c for c in base_df.select_dtypes(include='object').columns]
    cat_cols = [c for c in cat_cols if c != metric]
    num_cols = [c for c in base_df.columns if c not in cat_cols and c != metric]
    #eda_vars = list(final_vars + importance_vars + duns_date_cols + mandatory_cols + [metric, 'mail_date'])
    eda_vars = list(final_vars + importance_vars  + mandatory_cols)

    df_fe, fe_woeDF, log_cols = data_preparation.modeling_data_preprocessing_table(df = base_df, 
                                                        label = metric, 
                                                        cat_cols = cat_cols, 
                                                        num_cols = num_cols,
                                                        var_cols = eda_vars, 
                                                        type_balanced = type_balanced, 
                                                        flg_log = flg_log, 
                                                        flg_woe = flg_woe)
                                                    
    model = nf_model_automl_binary()
    model.catboost_hyper = custom_catboost_hyper
    model.lgbm_hyper = custom_lgbm_hyper
    model.xgb_hyper = custom_xgb_hyper

    # set model parameters
    model.top_n_prnct = 0.90
    model.missing_threshold = 0.75
    model.correlation_threshold = 0.8
    model.vif_threshold = 4
    #model.drop_list = drop_list
    model.fixed_vars = mandatory_cols
    model.categorical_vars = [] # categorical variables cannot be used with mode as 'xgb' 
    model.stratify_vars = []  # list of columns to have stratify split of train-test 
    model.max_features = 30  ## upper-limit/maximum number of columns (excluding fixed_vars) to shortlist in eda_fit_feature_selection
    model.kfolds = 7  # (0 --> does not run k-fold ; higher the value more time it will take to run)
    #model.mode = mode
    model.comp_against = {} 
    #model.exclusion_list = [c for c in ['duns', 'mail_date', 'load_year_prior', 'load_month_prior', 'campaignid', 'accountnum', 'r', 'load_year', 'load_month', 'flg_resp', 'flg_qual', 'flg_sub', 'flg_appr', 'flg_fund', 'flg_NFappr', 'flg_NFfund', 'fund_amt', 'NFfund_amt', 'funded_margin', 'NFfund_margin', 'ucc_filing_date', 'ref_score'] if (c != metric) & (c in df_fe.columns)]  # list of comp_against vars not to include in model building, just to be considered for comparison purpose
    model.exclusion_list = []  

    model.test_size = 0.2 
    #model.input_duns_col = "duns"
    model.target = metric
    model.param_dict = {
        #'weight': weight,
        'n_iter': 7,  # number of model user wants to train as part of hyper-parameter tuning
        'n_jobs': -1  # number of cores user wants to use during model training. -1 means it will use all the available cores in driver node
    }
    model.imp_woe = fe_woeDF
    model.log_cols = log_cols

    model.eda_fit_feature_selection(df_fe, execution=eda_fit_feature_selection_execution)


    # ----------------------------------------------Hyper-parameter tuning  --------------------------------------------
    model.df_pandas = model.df_pandas.loc[:, ~model.df_pandas.columns.duplicated()]
    model.X = [x for x in model.X if x in model.df_pandas.columns]
    model.fit_multi_iter(model.df_pandas)


    #------------------------------------ performance comparison chart, choosing best model---------------------------------------------------
    chosen_model = model.best_iter_comp(ctb_nth_iter = None, lgb_nth_iter = None, xgb_nth_iter = None, best_model_measure='auc')
    model.metric_comparison_report()
    model.fit(final_model = chosen_model)
    model.enable_report()
    model.generate_report2()
    model.generate_charts()

  
    # ------------------------------------- Save the model artifacts for production ---------
    if save_model_artifacts:
        mlflow_info = model.log_this_iteration()
        model_name = model_name_flg(model_name, flg_ags, type_balanced, flg_log, flg_woe)

    # --------------------------------------- Final Model Evaluation Metrics ----------------------------------------------

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

    final_model = model.finalmodel
    preds= model.X

    # for c in modeling_df.columns:
    #     col_type = modeling_df[c].dtype
    #     print(c,col_type)

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

    try:
        shap_val = shap_values[1]
        shap.summary_plot(shap_val,modeling_df[preds],max_display=35, plot_size=[20,9])
    except:
        shap_val = shap_values
        shap.summary_plot(shap_val,modeling_df[preds],max_display=35, plot_size=[20,9])

    # Plotting the distribution and target rate for each numeric variable
    num_plots = len(model.X)
    n_cols = 2  # Number of columns of subplots
    num_rows = (num_plots + n_cols - 1) // n_cols  # Calculate number of rows needed

    # Plot each feature's partial dependence
    feature_importance['abs_mean'] = feature_importance['feature_importance_vals'].abs().mean()
    feature_order = feature_importance.sort_values(['abs_mean'], ascending=False)['col_name']
    for i, colname in enumerate(feature_order):
        row = i // n_cols
        col = i % n_cols
        cutoff = modeling_df[colname].quantile(0.95)

        # Plot partial dependence for the current feature
        shap.plots.partial_dependence(
            colname, model.finalmodel.predict, modeling_df[model.X], ice=False,
            model_expected_value=True, feature_expected_value=True, xmax=cutoff
        )

    plt.tight_layout()
    plt.show()
