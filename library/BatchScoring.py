# ============================================================
# Batch Scoring (Portable Python Version)
# ============================================================

import os
import numpy as np
import pandas as pd
import logging

# ------------------------------------------------------------
# REQUIRED EXTERNAL DEPENDENCIES
# ------------------------------------------------------------
# These must already be available in your environment:
#
# - scoring()          -> from prod_mlflow_model_scoring.py
# - MLFLOW_KEYS        -> from your config file
#
# Example:
# from prod_mlflow_model_scoring import scoring
# from config import MLFLOW_KEYS
# ------------------------------------------------------------


# ============================================================
# BATCH SCORING CLASS
# ============================================================

class BatchScoring:
    """
    Handler for batch model scoring.
    """

    def __init__(self, session_obj=None):
        """
        session_obj is optional.
        If not provided, a default logger is used.
        """
        if session_obj is not None:
            self.__session = session_obj
            self.log = session_obj.log
        else:
            self.__session = None
            self.log = logging.getLogger("BatchScoring")
            if not self.log.handlers:
                logging.basicConfig(level=logging.INFO)

        self.log.info("Initializing BatchScoring object")

    # --------------------------------------------------------
    # Identify model list
    # --------------------------------------------------------
    def identify_model_list(
        self,
        modeling_data_source,
        source_tab_brand,
        class_type,
        model_type,
        ppp,
        version
    ):
        """
        Decide segment and model list.
        """
        self.modeling_data_source = modeling_data_source
        self.source_tab_brand = source_tab_brand
        self.class_type = class_type
        self.model_type = model_type
        self.version = version
        self.list_model = []

        flg_ppp = "nonppp" if ppp == "N" else "ppp"

        if model_type == "RESP":
            self.list_model = [
                mdl for mdl in MLFLOW_KEYS[version].keys()
                if source_tab_brand in mdl
                and class_type in mdl
                and "RESP" in mdl
                and (f"_{flg_ppp}_" in mdl or version == "V50")
            ]

        elif model_type == "SUB":
            self.list_model = [
                mdl for mdl in MLFLOW_KEYS[version].keys()
                if source_tab_brand in mdl
                and class_type in mdl
                and "SUB" in mdl
                and (f"_{flg_ppp}_" in mdl or version == "V50")
            ]

        elif model_type == "APPR":
            self.list_model = [
                mdl for mdl in MLFLOW_KEYS[version].keys()
                if source_tab_brand in mdl and "APPR" in mdl
            ]

        elif model_type == "FUND":
            self.list_model = [
                mdl for mdl in MLFLOW_KEYS[version].keys()
                if source_tab_brand in mdl and "FUND" in mdl
            ]

        elif model_type == "fundamt":
            self.list_model = [
                mdl for mdl in MLFLOW_KEYS[version].keys()
                if source_tab_brand in mdl and "fundamt" in mdl
            ]

        self.log.info(f"Identified model list: {self.list_model}")

    # --------------------------------------------------------
    # Generate scores for one model
    # --------------------------------------------------------
    def generate_scores(self, data, model_name, version):

        mlflow_keys = MLFLOW_KEYS[version][model_name]

        run_id = mlflow_keys[0]
        local_path = mlflow_keys[1]   # kept for compatibility
        final_model_mode = mlflow_keys[2]

        if final_model_mode == "lgb":
            artifact_path = "lightgbm_model"
        elif final_model_mode == "ctb":
            artifact_path = "catboost_model"
        elif final_model_mode == "xgb":
            artifact_path = "xgboost_model"
        else:
            raise ValueError("Unknown model mode")

        logged_model = f"runs:/{run_id}/{artifact_path}"

        score_output = scoring(
            logged_model_uri=logged_model,
            rawdf=data,
            final_model_mode=final_model_mode,
            exclude_from_feature_engineering_cols=["duns"],
            ags_model=(model_name == "AGS_Model")
        )

        return score_output

    # --------------------------------------------------------
    # Score one segment (chunked)
    # --------------------------------------------------------
    def score_segment(self, baseline_data, mdl_name, version):

        def df_in_chunks(df, row_count):
            chunks = []
            n = len(df) // row_count + 1
            for i in range(n):
                chunks.append(df.iloc[i * row_count:(i + 1) * row_count])
            return chunks

        output_all = pd.DataFrame()
        chunk_size = 5_000_000

        for df_chunk in df_in_chunks(baseline_data, chunk_size):
            output = self.generate_scores(df_chunk, mdl_name, version)
            output_all = pd.concat([output_all, output], axis=0)

        self.log.info(f"Finished scoring model: {mdl_name}")
        self.log.info(f"Unique DUNS scored: {output_all['duns'].nunique():,}")

        return output_all

    # --------------------------------------------------------
    # Score all models in batch
    # --------------------------------------------------------
    def score_batch(self, version):

        dffs = pd.DataFrame()

        for mdl_name in self.list_model:

            self.log.info(f"Start scoring segment: {mdl_name}")

            class_type = mdl_name[-2:]

            if "RESP" in mdl_name or "SUB" in mdl_name:
                data_seg = self.modeling_data_source[
                    self.modeling_data_source["class_type"] == class_type
                ]
            elif "APPR" in mdl_name or "fundamt" in mdl_name:
                data_seg = self.modeling_data_source
            else:
                self.log.error(f"[ERROR] No such segment for model {mdl_name}")
                continue

            self.log.info(f"Generated data segment for model {mdl_name}")

            dff = self.score_segment(data_seg, mdl_name, version)
            dff["mdl_name"] = mdl_name.rsplit("_", 1)[0]

            dffs = pd.concat(
                [dffs, dff[["duns", "proba", "mdl_name"]]],
                axis=0
            )

        return dffs
