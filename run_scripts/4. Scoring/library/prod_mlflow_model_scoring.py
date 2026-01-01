# Databricks notebook source
import ast
import mlflow
from mlflow.tracking import MlflowClient
import pickle
from DBAutoml import *

# COMMAND ----------

def gendf_capping(orig_df, dict):
    """
    perform capping based on the provided dictionary
    """
    output_df = orig_df.copy()
    for ck, cv in dict.items():
        output_df[ck] = orig_df[ck].clip(lower=cv[0], upper = cv[1])
    return output_df

# COMMAND ----------

def scoring(logged_model, rawdf, final_model_mode, exclude_from_feature_engineering_cols, ags_model = False):
    #########################################################
    # load model #
    #########################################################
    if final_model_mode == 'lgb':
        loaded_model = mlflow.lightgbm.load_model(logged_model)
    elif final_model_mode == 'ctb':
        loaded_model = mlflow.catboost.load_model(logged_model)
    elif final_model_mode == 'xgb':
        loaded_model = mlflow.xgboost.load_model(logged_model)
        
    run_id = logged_model.split('/')[-2]

    #########################################################
    # load imp_woe/log_cols #
    #########################################################
    client = MlflowClient()

    if ags_model is False:
        # imp_poe
        tmp_loc = client.download_artifacts(run_id, "imp_woe.pkl")
        with open(tmp_loc, 'rb') as handle:
            imp_woe = pickle.load(handle)

    # get needed params
    run_data_dict = client.get_run(run_id).data.to_dictionary()
    cols_catg = ast.literal_eval(run_data_dict['params']['categorical_vars'])
    
    cols_df = rawdf.columns
    
    #########################################################
    # apply imp_woe and log transformation
    #########################################################
    if final_model_mode == 'lgb':
        f_names = loaded_model.feature_name_
    elif final_model_mode == 'ctb':
        f_names = loaded_model.feature_names_
    elif final_model_mode == 'xgb':
        f_names = loaded_model.feature_names_
    
    if ags_model is False:
        if imp_woe is not None:
            for k in imp_woe['Variable'].unique():
                crstb = imp_woe[imp_woe['Variable']==k]
                rawdf[k].fillna('None', inplace=True)
                rawdf[k] = rawdf[k].map(crstb.set_index('Cutoff')['WoE'])  
        else:
            for k in cols_catg:
                rawdf[k].fillna('None', inplace=True)
        
    #########################################################
    # validate categorical vars #
    #########################################################
    if cols_catg is not None:
        # ------------------------------ validate categorical vars
        cols_not_in_df = [x for x in cols_catg if x not in cols_df]
        if len(cols_not_in_df) > 0:
            raise KeyError(f"categorical variables {cols_not_in_df} are not present in data")

        # ------------------------------ convert categorical columns used during model building to str/category
        if isinstance(loaded_model, ctb.CatBoostClassifier):
            for colmn in cols_catg:
                try:
                    rawdf[colmn] = rawdf[colmn].astype(str)
                except:
                    raise TypeError(
                        f"categorical column: '{colmn}' is not converted to Type String. Check your data and retry")
        elif isinstance(loaded_model, lgb.LGBMClassifier) or isinstance(loaded_model, xgb.XGBClassifier):
            for colmn in cols_catg:
                try:
                    rawdf[colmn] = rawdf[colmn].astype("category")
                except:
                    raise TypeError(
                        f"categorical column: '{colmn}' is not converted to Type Category. Check your data and retry")
        else:
            raise TypeError(
                f"Parameter 'model' must be an object among CatBoostClassifier, LGBMClassifier or XGBClassifier.")

    else:
        pass

    #########################################################
    # scoring #
    #########################################################
    rawdf['proba'] = loaded_model.predict_proba(rawdf[f_names])[:,1]

    return rawdf