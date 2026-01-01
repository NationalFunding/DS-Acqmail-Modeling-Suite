# ============================================================
# AutoML Utilities – Portable Python Version
# ============================================================

from itertools import chain
import numpy as np
import pandas as pd
import seaborn as sns
import os
import ast
import pickle
import shap

from sklearn import metrics
from scipy.stats import pearsonr

import mlflow
from mlflow.tracking import MlflowClient

# ------------------------------------------------------------
# NOTE:
# - DBAutoml imports must be explicit in Python
# - Spark, dbutils, and notebook globals are removed
#
# Example:
# from dbautoml import *
# ------------------------------------------------------------


# ============================================================
# LOG / TRANSFORM SUGGESTIONS
# ============================================================

def log_suggestion(df, var_lst):
    indicator_like_boolean = df[var_lst].nunique() == 2
    indicator_like_cols = indicator_like_boolean[indicator_like_boolean].index.values
    cols_num = [c for c in var_lst if c not in indicator_like_cols]

    col_skew_boolean = df[cols_num].skew() > 1
    log_cols = col_skew_boolean[col_skew_boolean].index.values
    min_0_boolean = df[log_cols].min() >= 0
    final_log_cols = min_0_boolean[min_0_boolean].index.values

    return final_log_cols


# ============================================================
# BASELINE / MAILING EXPERIMENT
# ============================================================

def baseline_mailing_experiment(df, label):

    baseline_cols = [
        'non_fin_trade_balance','comptype_hq','bus_strt_yr','d_sat_hq',
        'pexp_30','pexp_sat','pydx_1','totdoll_hq','amtowed30',
        'nbrpayexpsslow','pr_bankruptcy_ind','buydex'
    ]

    mailed_cols = [
        'cadence_pattern','timesmailed','months_since_first',
        'months_since_last','flg_mailed','mailed_6mo','mailed_12mo'
    ]

    if label == 'flg_resp':
        mandatory_cols = ['TIB','ags_model_250k','mail_date','flg_mailed','chg_flg','duns']
    else:
        mandatory_cols = ['TIB','ags_model_250k','mail_date','chg_flg','duns']

    combined_cols = list(set(baseline_cols + mailed_cols + mandatory_cols + [label]))
    combined_cols = [c for c in combined_cols if c in df.columns]

    return df[combined_cols]


# ============================================================
# WEIGHTING / BALANCING
# ============================================================

def weighted_func(df, label):
    weight = 'wt'
    pct_dict = df[label].value_counts(normalize=True).to_dict()
    weight_dict = {1: 0.5 / pct_dict[1], 0: 0.5 / pct_dict[0]}
    df[weight] = df[label].map(weight_dict)
    return df


def flg_balanced(df, type_balance, label):
    weight = 'wt'

    if type_balance is None:
        df[weight] = 1
        return df

    if label is None:
        raise ValueError("Label is required for balancing")

    df_1 = df[df[label] == 1]
    df_0 = df[df[label] == 0]

    ratio_map = {
        'ratio1_2': 2,
        'ratio1_3': 3,
        'ratio1_4': 4,
        'ratio1_5': 5,
        'ratio1_10': 10,
        'ratio1_25': 2.5
    }

    if type_balance == 'balanced':
        size = min(len(df_1), len(df_0))
        df = pd.concat([
            df_1.sample(size, random_state=1),
            df_0.sample(size, random_state=1)
        ])
        df[weight] = 1
        return df

    if type_balance in ratio_map:
        ratio = ratio_map[type_balance]
        if len(df_1) < len(df_0):
            df = pd.concat([df_1, df_0.sample(int(len(df_1) * ratio), random_state=1)])
        else:
            df = pd.concat([df_0, df_1.sample(int(len(df_0) * ratio), random_state=1)])
        df[weight] = 1
        return df

    if type_balance.endswith('_weighted'):
        df = flg_balanced(df, type_balance.replace('_weighted',''), label)
        return weighted_func(df, label)

    if type_balance == 'weighted':
        return weighted_func(df, label)

    raise ValueError(f"Unknown balancing option: {type_balance}")


# ============================================================
# IV / WOE
# ============================================================

def iv_woe(data, target, smoothing_factor=10):

    iv_df = []
    woe_df = []

    for col in data.columns:
        if col == target:
            continue

        d = data.groupby(col)[target].agg(['count','sum']).reset_index()
        d.columns = ['Cutoff','N','Events']
        d['NonEvents'] = d['N'] - d['Events']

        d['Events'] = d['Events'].clip(lower=0.5)
        d['NonEvents'] = d['NonEvents'].clip(lower=0.5)

        d['WoE'] = np.log(
            (d['Events'] / d['Events'].sum()) /
            (d['NonEvents'] / d['NonEvents'].sum())
        )

        d['IV'] = (d['WoE'] * (
            d['Events']/d['Events'].sum() -
            d['NonEvents']/d['NonEvents'].sum()
        ))

        iv_val = d['IV'].sum()

        iv_df.append({
            'Variable': col,
            'IV': iv_val
        })

        d.insert(0, 'Variable', col)
        woe_df.append(d)

    return pd.DataFrame(iv_df), pd.concat(woe_df)


def woe_cols(df, cat_cols, label):
    _, woeDF = iv_woe(df[cat_cols + [label]].fillna('None'), label)

    for col in cat_cols:
        crstb = woeDF[woeDF['Variable'] == col]
        df[col] = df[col].map(crstb.set_index('Cutoff')['WoE'])

    return df, woeDF


# ============================================================
# MODELING DATA PREPROCESSING
# ============================================================

def modeling_data_preprocessing_table(
    df,
    label,
    cat_cols,
    num_cols,
    var_cols,
    flg_baseline=False,
    type_balanced=None,
    flg_log=False,
    flg_woe=False
):

    if flg_baseline:
        df = baseline_mailing_experiment(df, label)
    else:
        df = df[var_cols]

    df = flg_balanced(df, type_balanced, label)

    if flg_woe:
        df, woeDF = woe_cols(df, [c for c in cat_cols if c in df.columns], label)
    else:
        woeDF = None
        df[cat_cols] = df[cat_cols].fillna('None')

    if flg_log:
        log_cols = log_suggestion(df, num_cols)
        for col in log_cols:
            df[col] = np.log(df[col] + 1)
    else:
        log_cols = None

    return df, woeDF, log_cols
# ============================================================
# HOLDOUT DATA PREPROCESSING
# ============================================================

def holdout_data_preprocessing_table(table_name, model_vars, cols_catg, log_cols, woeDF, label = None, use_spark_table = False):

    # exclude_from_feature_engineering_cols = ['duns', 'mail_date', 'load_year_prior', 'load_month_prior', 'campaignid', 'accountnum', 'r', 'load_year', 'load_month', 'flg_resp', 'flg_qual', 'flg_sub', 'flg_appr', 'flg_fund', 'flg_NFappr', 'flg_NFfund', 'fund_amt', 'NFfund_amt', 'funded_margin', 'NFfund_margin', 'ucc_filing_date']+[label]
    exclude_from_feature_engineering_cols = [label]
    
    dfPandas = table_name
    dfPandas = dfPandas[list(set(model_vars + exclude_from_feature_engineering_cols))]
    

    if woeDF is not None:  
        woe_cols = [x for x in woeDF['Variable'].unique() if x in dfPandas.columns]
        dfPandas[woe_cols] = dfPandas[woe_cols].fillna('None')
        for k in woe_cols:
            crstb = woeDF[woeDF['Variable']==k]
            dfPandas[k] = dfPandas[k].map(crstb.set_index('Cutoff')['WoE'])
    else:
        dfPandas[cols_catg] = dfPandas[cols_catg].fillna('None')

    if log_cols is not None:
        model_log = [x for x in log_cols if x in dfPandas.columns]
        for col in model_log:
            dfPandas[col] = dfPandas[col].apply(lambda x: np.log(x+1))

    return dfPandas


# ============================================================
# LOAD MODEL FROM MLFLOW
# ============================================================

def load_model_from_mlflow(run_id, final_model_mode):

    if final_model_mode == 'lgb':
        artifact_path = 'lightgbm_model'
        model = mlflow.lightgbm.load_model(f"runs:/{run_id}/{artifact_path}")

    elif final_model_mode == 'ctb':
        artifact_path = 'catboost_model'
        model = mlflow.catboost.load_model(f"runs:/{run_id}/{artifact_path}")

    elif final_model_mode == 'xgb':
        artifact_path = 'xgboost_model'
        model = mlflow.xgboost.load_model(f"runs:/{run_id}/{artifact_path}")

    else:
        raise ValueError("Unsupported model type")

    return model
