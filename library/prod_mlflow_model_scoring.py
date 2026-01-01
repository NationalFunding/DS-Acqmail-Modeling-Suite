# ============================================================
# Scoring Utilities (Portable Python Version)
# ============================================================

import ast
import pickle
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient

# ------------------------------------------------------------
# Optional model imports (used only for isinstance checks)
# ------------------------------------------------------------
import lightgbm as lgb
import catboost as ctb
import xgboost as xgb


# ============================================================
# CAPPING
# ============================================================

def gendf_capping(orig_df, cap_dict):
    """
    Perform capping based on the provided dictionary.

    cap_dict format:
      {
        "col_name": (lower_bound, upper_bound)
      }
    """
    output_df = orig_df.copy()
    for col, bounds in cap_dict.items():
        output_df[col] = output_df[col].clip(lower=bounds[0], upper=bounds[1])
    return output_df


# ============================================================
# SCORING
# ============================================================

def scoring(
    logged_model_uri,
    rawdf,
    final_model_mode,
    exclude_from_feature_engineering_cols=None,
    ags_model=False
):
    """
    Score a dataset using an MLflow-logged model.

    Parameters
    ----------
    logged_model_uri : str
        MLflow model URI (e.g. "runs:/<run_id>/model")
    rawdf : pandas.DataFrame
        Input dataframe to score
    final_model_mode : str
        One of {'lgb', 'ctb', 'xgb'}
    exclude_from_feature_engineering_cols : list or None
        (Kept for compatibility; not used here)
    ags_model : bool
        If True, skips WoE logic
    """

    # --------------------------------------------------------
    # Load model
    # --------------------------------------------------------
    if final_model_mode == 'lgb':
        loaded_model = mlflow.lightgbm.load_model(logged_model_uri)
    elif final_model_mode == 'ctb':
        loaded_model = mlflow.catboost.load_model(logged_model_uri)
    elif final_model_mode == 'xgb':
        loaded_model = mlflow.xgboost.load_model(logged_model_uri)
    else:
        raise ValueError("final_model_mode must be one of ['lgb', 'ctb', 'xgb']")

    # Extract run_id from MLflow URI
    # Example: runs:/<run_id>/model
    run_id = logged_model_uri.split("/")[-2]

    client = MlflowClient()

    # --------------------------------------------------------
    # Load imp_woe (if applicable)
    # --------------------------------------------------------
    imp_woe = None
    if not ags_model:
        try:
            tmp_loc = client.download_artifacts(run_id, "imp_woe.pkl")
            with open(tmp_loc, "rb") as f:
                imp_woe = pickle.load(f)
        except Exception:
            imp_woe = None

    # --------------------------------------------------------
    # Get categorical variables from run params
    # --------------------------------------------------------
    run_data = client.get_run(run_id).data.to_dictionary()
    cols_catg = ast.literal_eval(run_data["params"].get("categorical_vars", "[]"))

    cols_df = rawdf.columns.tolist()

    # --------------------------------------------------------
    # Get feature names used by model
    # --------------------------------------------------------
    if final_model_mode == 'lgb':
        feature_names = loaded_model.feature_name_
    elif final_model_mode == 'ctb':
        feature_names = loaded_model.feature_names_
    elif final_model_mode == 'xgb':
        feature_names = loaded_model.feature_names_

    # --------------------------------------------------------
    # Apply WoE transformation (if applicable)
    # --------------------------------------------------------
    if not ags_model:
        if imp_woe is not None:
            for var in imp_woe["Variable"].unique():
                if var not in rawdf.columns:
                    continue
                crstb = imp_woe[imp_woe["Variable"] == var]
                rawdf[var] = rawdf[var].fillna("None")
                rawdf[var] = rawdf[var].map(
                    crstb.set_index("Cutoff")["WoE"]
                )
        else:
            for col in cols_catg:
                if col in rawdf.columns:
                    rawdf[col] = rawdf[col].fillna("None")

    # --------------------------------------------------------
    # Validate and cast categorical variables
    # --------------------------------------------------------
    if cols_catg:
        missing_cols = [c for c in cols_catg if c not in cols_df]
        if missing_cols:
            raise KeyError(f"Categorical variables missing from data: {missing_cols}")

        if isinstance(loaded_model, ctb.CatBoostClassifier):
            for col in cols_catg:
                rawdf[col] = rawdf[col].astype(str)

        elif isinstance(loaded_model, (lgb.LGBMClassifier, xgb.XGBClassifier)):
            for col in cols_catg:
                rawdf[col] = rawdf[col].astype("category")

        else:
            raise TypeError(
                "Model must be CatBoostClassifier, LGBMClassifier, or XGBClassifier"
            )

    # --------------------------------------------------------
    # Scoring
    # --------------------------------------------------------
    rawdf = rawdf.copy()
    rawdf["proba"] = loaded_model.predict_proba(rawdf[feature_names])[:, 1]

    return rawdf
