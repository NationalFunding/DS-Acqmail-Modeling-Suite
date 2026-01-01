# nf_model_automl_binary.py
# ------------------------------------------------------------
# Standalone, non-Databricks refactor
# ------------------------------------------------------------

import os
import io
import pickle
import tempfile
import warnings
from datetime import datetime
from time import time
import builtins
from base64 import b64encode

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn import metrics
from IPython.display import display, HTML

from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_curve,
    auc,
    roc_auc_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    accuracy_score,
    f1_score
)
import shap


import lightgbm as lgb
import mlflow
from library.NFAutoml import feature_engg, ks_search,mode_catboost_bn,mode_lightgbm_bn,mode_xgboost_bn
#from feature_engg import feature_engg
#from ks_search import ks_search
# from mode_catboost_bn import mode_catboost_bn
# from mode_lightgbm_bn import mode_lightgbm_bn
# from mode_xgboost_bn import mode_xgboost_bn

# ------------------------------------------------------------
warnings.filterwarnings("ignore")
plt.rcParams.update({'figure.max_open_warning': 0})


class InsufficientRecordsError(Exception):
    pass


class nf_model_automl_binary:
    """
    Standalone AutoML Binary Classifier (Pandas-only)
    """

    def __init__(self):
        self.top_n_prnct = 0.9
        self.missing_threshold = 0.99
        self.correlation_threshold = 0.8
        self.vif_threshold = 5
        self.pdo = 40
        self.basepoints = 1130
        self.mode = ['ctb', 'lgb', 'xgb']
        self.weight = None
        #self.input_duns_col = 'duns'
        self.drop_list = []
        self.fixed_vars = []
        self.categorical_vars = []
        self.stratify_vars = []
        self.drop_reason = {}
        self.max_features = 10
        self.kfolds = 0
        self.target = None
        self.param_dict = None
        self.comp_against = {'probability': None}
        self.exclusion_list = []
        self.seed = 25
        self.df_pandas = None
        self.X = []
        self.flag_train_test_split = True
        self.flag_eda_required = True
        self.imp_woe = None
        self.log_cols = None

    # ------------------------------------------------------------------
    # INPUT VALIDATION
    # ------------------------------------------------------------------
    def input_parameters_validation(self, df_input):
        if not isinstance(df_input, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")

        if df_input.shape[0] < 100:
            raise InsufficientRecordsError("Minimum 100 records required")
        
        df_input = df_input.loc[:, ~df_input.columns.duplicated()]
        self.df_pandas = df_input

        # if self.input_duns_col not in df_input.columns:
        #     raise KeyError(f"Missing duns column: {self.input_duns_col}")

        # self.df_pandas = df_input.sort_values(self.input_duns_col).copy()

        if self.target not in self.df_pandas.columns:
            raise KeyError(f"Missing target column: {self.target}")

        self.df_pandas[self.target] = self.df_pandas[self.target].astype(int)

        if self.weight is None:
            self.df_pandas['wt'] = 1
            self.weight = 'wt'

        if self.weight not in self.df_pandas.columns:
            raise KeyError(f"Missing weight column: {self.weight}")

        return True

    # ------------------------------------------------------------------
    # FEATURE SELECTION
    # ------------------------------------------------------------------
    def eda_fit_feature_selection(self, df_input,execution=True):
        self.input_parameters_validation(df_input)

        self.fe = feature_engg(
            self.df_pandas,
            self.target,
            self.weight,
            self.missing_threshold,
            self.correlation_threshold,
            self.vif_threshold,
            self.fixed_vars,
            self.exclusion_list
        )

        self.X = self.fe.run_feature_engg()
        self.drop_reason.update(self.fe.drop_reason)

        self.flag_eda_required = False
        self.__create_final_model_vars()

     ############## to update model param_dict ##############
    def update_model_parameter_dict(self, df):

        if self.flag_train_test_split:
            train_df, test_df = train_test_split(
                df[self.model_vars],
                test_size=self.param_dict['test_size'],
                stratify=df[self.target],
                random_state=self.seed
            )
        else:
            train_df = df[self.model_vars]
            test_df  = self.test_size[self.model_vars]

        # 🔑 CRITICAL: reset index to avoid alignment bugs
        train_df = train_df.reset_index(drop=True)
        test_df  = test_df.reset_index(drop=True)

        # remove stratify column if present
        stratify_col = getattr(self, "stratify_col", None)
        if stratify_col and stratify_col in train_df.columns:
            train_df = train_df.drop(columns=[stratify_col])
            test_df  = test_df.drop(columns=[stratify_col])
            if stratify_col in self.model_vars:
                self.model_vars.remove(stratify_col)
                print(f"[INFO] Removed intermediate feature '{stratify_col}' from 'model.model_vars'.")

        self.df_train = train_df
        self.df_test  = test_df

        print(
            f"[INFO] shape of train dataset is {self.df_train.shape} "
            f"\n\t * shape of test dataset is {self.df_test.shape}"
        )

        # parallelism (kept for compatibility, not used)
        n_parallel_models = max(2, min(16, int(self.param_dict['n_iter'] / 4)))

        self.param_dict.update({
            'n_parallel_models': n_parallel_models,
            'target': self.target,
            'weight': self.weight,
            'train': self.df_train,
            'test': self.df_test,
            'X': self.X,
            'y': self.target,
            'catg_features': self.cols_catg,
            'kfolds': self.kfolds
        })

        
        assert len(self.df_train[self.X]) == len(self.df_train[self.target]), \
            f"Train X/y length mismatch: {len(self.df_train[self.X])} vs {len(self.df_train[self.target])}"

        assert len(self.df_test[self.X]) == len(self.df_test[self.target]), \
            f"Test X/y length mismatch: {len(self.df_test[self.X])} vs {len(self.df_test[self.target])}"

        assert len(self.df_train[self.weight]) == len(self.df_train[self.target]), \
            "Train weight/y length mismatch"

        assert self.df_train.index.is_monotonic_increasing, "Train index not reset"
        assert self.df_test.index.is_monotonic_increasing, "Test index not reset"

        return True


    def __create_final_model_vars(self):
        self.X = list(dict.fromkeys(self.X))
        self.X = [c for c in self.X if c not in {self.target, self.weight}]
        self.model_vars = self.X + [self.weight, self.target]
        self.cols_numeric = self.df_pandas[self.X].select_dtypes(include=np.number).columns.tolist()
        self.cols_catg = [x for x in self.X if x not in self.cols_numeric]

    # ------------------------------------------------------------------
    # MODEL TRAINING
    # ------------------------------------------------------------------
    def fit_multi_iter(self, df):
        if self.flag_eda_required:
            self.input_parameters_validation(df)
            self.__create_final_model_vars()

        self.update_model_parameter_dict(df)

        if 'ctb' in self.mode:
            print("\n---------------------- CatBoost Hyper-parameters tuning started ----------------------")
            self.ctbmodel = mode_catboost_bn(df, self.param_dict)
            if hasattr(self, "catboost_hyper"):
                self.ctbmodel.custom_hyper_param_space = self.catboost_hyper
            self.ctbmodel.fit_multi_iter()

        if 'lgb' in self.mode:
            print("\n---------------------- LightGBM Hyper-parameters tuning started ----------------------")
            self.lgbmodel = mode_lightgbm_bn(df, self.param_dict)
            if hasattr(self, "lgbm_hyper"):
                self.lgbmodel.custom_hyper_param_space = self.lgbm_hyper
            self.lgbmodel.fit_multi_iter()

        if 'xgb' in self.mode:
            print("\n---------------------- XGBoost Hyper-parameters tuning started ----------------------")
            self.xgbmodel = mode_xgboost_bn(df, self.param_dict)
            if hasattr(self, "xgb_hyper"):
                self.xgbmodel.custom_hyper_param_space = self.xgb_hyper
            self.xgbmodel.fit_multi_iter()


    def best_iter_comp(self, ctb_nth_iter=None, lgb_nth_iter=None, xgb_nth_iter=None, best_model_measure='auc'):
        """
        function to retrain the final model using the hyper-parameters present in nth iteration. for comparison purpose

        Input Parameters
        ----------
          ctb_nth_iter : integer or None
            Catboost iteration number for final model
          lgb_nth_iter : integer or None
            LightGBM iteration number for final model
          xgb_nth_iter : integer or None
            XGBoost iteration number for final model
        """

        # Define a result dict to store performance metrices
        result_dict = {'classifiers': [], 'false_positive_rate': [], 'true_positive_rate': [], 'auc': [], 'f1_score': [], 'recall': [], 'precision': []}
        # add precision recall to automl binary AAS-874
        result_dict_pr = {'classifiers': [], 'precision': [], 'recall': [], 'auc': []}
        self.models_list = []

        if 'ctb' in self.mode:
            print("\n---------------------- Catboost Final Training started ----------------------")
            self.ctbmodel.fit(ctb_nth_iter)
            self.models_list.append(self.ctbmodel.final_model)

            # calculate metric results to show roc_auc curve
            y_prob_ctb = self.ctbmodel.final_model.predict_proba(self.ctbmodel.test[self.ctbmodel.X])[:, 1]
            y_pred_ctb = self.ctbmodel.final_model.predict(self.ctbmodel.test[self.ctbmodel.X])
            false_positive_rate_ctb, true_positive_rate_ctb, thresholds = roc_curve(self.ctbmodel.test[self.ctbmodel.y],
                                                                                    y_prob_ctb)
            auc_roc_ctb = auc(false_positive_rate_ctb, true_positive_rate_ctb)
            roc_auc_ctb = metrics.roc_auc_score(self.ctbmodel.test[self.ctbmodel.y], y_prob_ctb)
            precision_metric_ctb = metrics.precision_score(self.ctbmodel.test[self.ctbmodel.y], y_pred_ctb)
            recall_metric_ctb = metrics.recall_score(self.ctbmodel.test[self.ctbmodel.y], y_pred_ctb)
            
            # calculate precision recall results to show pr curve AAS-874
            precision_ctb, recall_ctb, thresholds = precision_recall_curve(self.ctbmodel.test[self.ctbmodel.y],
                                                                                    y_prob_ctb)
            auc_pr_ctb = auc(recall_ctb, precision_ctb)

            # append results to dictionary
            result_dict['classifiers'].append('ctb')
            result_dict['false_positive_rate'].append(false_positive_rate_ctb)
            result_dict['true_positive_rate'].append(true_positive_rate_ctb)
            result_dict['auc'].append(auc_roc_ctb)
            result_dict['f1_score'].append((2*precision_metric_ctb*recall_metric_ctb)/(precision_metric_ctb+recall_metric_ctb))
            result_dict['precision'].append(precision_metric_ctb)
            result_dict['recall'].append(recall_metric_ctb)
            
            # append results to dictionary AAS-874
            result_dict_pr['classifiers'].append('ctb')
            result_dict_pr['precision'].append(precision_ctb)
            result_dict_pr['recall'].append(recall_ctb)
            result_dict_pr['auc'].append(auc_pr_ctb)

        if 'lgb' in self.mode:
            print("\n\n---------------------- LightGBM Final Training started ----------------------")
            self.lgbmodel.fit(lgb_nth_iter)
            self.models_list.append(self.lgbmodel.final_model)

            # calculate metric results to show roc_auc curve
            y_prob_lgb = self.lgbmodel.final_model.predict_proba(self.lgbmodel.test[self.lgbmodel.X])[:, 1]
            y_pred_lgb = self.lgbmodel.final_model.predict(self.lgbmodel.test[self.lgbmodel.X])
            false_positive_rate_lgb, true_positive_rate_lgb, thresholds = roc_curve(self.lgbmodel.test[self.lgbmodel.y],
                                                                                    y_prob_lgb)
            auc_roc_lgb = auc(false_positive_rate_lgb, true_positive_rate_lgb)
            roc_auc_lgb = metrics.roc_auc_score(self.lgbmodel.test[self.lgbmodel.y], y_prob_lgb)
            precision_metric_lgb = metrics.precision_score(self.lgbmodel.test[self.lgbmodel.y], y_pred_lgb)
            recall_metric_lgb = metrics.recall_score(self.lgbmodel.test[self.lgbmodel.y], y_pred_lgb)
            
            # calculate precision recall results to show pr curve AAS-874
            precision_lgb, recall_lgb, thresholds = precision_recall_curve(self.lgbmodel.test[self.lgbmodel.y],
                                                                                    y_prob_lgb)
            auc_pr_lgb = auc(recall_lgb, precision_lgb)

            # append results to dictionary
            result_dict['classifiers'].append('lgb')
            result_dict['false_positive_rate'].append(false_positive_rate_lgb)
            result_dict['true_positive_rate'].append(true_positive_rate_lgb)
            result_dict['auc'].append(auc_roc_lgb)
            result_dict['f1_score'].append((2*precision_metric_lgb*recall_metric_lgb)/(precision_metric_lgb+recall_metric_lgb))
            result_dict['precision'].append(precision_metric_lgb)
            result_dict['recall'].append(recall_metric_lgb)
            
            # append results to dictionary AAS-874
            result_dict_pr['classifiers'].append('lgb')
            result_dict_pr['precision'].append(precision_lgb)
            result_dict_pr['recall'].append(recall_lgb)
            result_dict_pr['auc'].append(auc_pr_lgb)

        if 'xgb' in self.mode:
            print("\n\n---------------------- XGBoost Final Training started ----------------------")
            self.xgbmodel.fit(xgb_nth_iter)
            self.models_list.append(self.xgbmodel.final_model)

            # calculte metric results to show roc_auc curve
            y_prob_xgb = self.xgbmodel.final_model.predict_proba(self.xgbmodel.test[self.xgbmodel.X])[:, 1]
            y_pred_xgb = self.xgbmodel.final_model.predict(self.xgbmodel.test[self.xgbmodel.X])
            false_positive_rate_xgb, true_positive_rate_xgb, thresholds = roc_curve(self.xgbmodel.test[self.xgbmodel.y],
                                                                                    y_prob_xgb)
            auc_roc_xgb = auc(false_positive_rate_xgb, true_positive_rate_xgb)
            roc_auc_xgb = metrics.roc_auc_score(self.xgbmodel.test[self.xgbmodel.y], y_prob_xgb)
            precision_metric_xgb = metrics.precision_score(self.xgbmodel.test[self.xgbmodel.y], y_pred_xgb)
            recall_metric_xgb = metrics.recall_score(self.xgbmodel.test[self.xgbmodel.y], y_pred_xgb)
            
            # calculate precision recall results to show pr curve AAS-874
            precision_xgb, recall_xgb, thresholds = precision_recall_curve(self.xgbmodel.test[self.xgbmodel.y],
                                                                                    y_prob_xgb)
            auc_pr_xgb = auc(recall_xgb, precision_xgb)

            # append results to dictionary
            result_dict['classifiers'].append('xgb')
            result_dict['false_positive_rate'].append(false_positive_rate_xgb)
            result_dict['true_positive_rate'].append(true_positive_rate_xgb)
            result_dict['auc'].append(auc_roc_xgb)
            result_dict['f1_score'].append((2*precision_metric_xgb*recall_metric_xgb)/(precision_metric_xgb+recall_metric_xgb))
            result_dict['precision'].append(precision_metric_xgb)
            result_dict['recall'].append(recall_metric_xgb)
            
            # append results to dictionary AAS-874
            result_dict_pr['classifiers'].append('xgb')
            result_dict_pr['precision'].append(precision_xgb)
            result_dict_pr['recall'].append(recall_xgb)
            result_dict_pr['auc'].append(auc_pr_xgb)

        # add no skill AAS-874
        testy = self.lgbmodel.test[self.lgbmodel.y]
        no_skill = len(testy[testy==1]) / len(testy)

        # create dataframe from results_dict
        result_table = pd.DataFrame(result_dict)
        result_table_pr = pd.DataFrame(result_dict_pr)

        print("===== F1 Score =====")
        print(dict(zip(result_dict['classifiers'], result_dict['f1_score'])))
        print("===== Precision =====")
        print(dict(zip(result_dict['classifiers'], result_dict['precision'])))
        print("===== Recall =====")
        print(dict(zip(result_dict['classifiers'], result_dict['recall'])))
        print("===== auc =====")
        print(dict(zip(result_dict['classifiers'], result_dict['auc'])))

        # highest_f1_index = result_dict['f1_score'].index(builtins.max(result_dict['f1_score']))
        # chosen_model = result_dict['classifiers'][highest_f1_index]
        highest_auc_index = result_dict['auc'].index(builtins.max(result_dict['auc']))
        chosen_model = result_dict['classifiers'][highest_auc_index]

        # plot roc_auc curve
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4.5), tight_layout=True)

        ax1.plot([0, 1], [0, 1], color='orange', linestyle='--')
        for i in result_table.index:
            ax1.plot(result_table.loc[i]['false_positive_rate'],
                     result_table.loc[i]['true_positive_rate'],
                     label="{}, auc_score={:.3f}".format(result_table.loc[i]['classifiers'],
                                                         result_table.loc[i]['auc']))

        ax1.legend(loc='lower right')
        ax1.set_title('Receiver Operating Characteristic')
        ax1.axis('tight')
        ax1.set_ylabel('True Positive Rate')
        ax1.set_xlabel('False Positive Rate')
        ax1.set_xlim(0,1)
        ax1.set_ylim(0,1)
        ax2.plot([0, 1], [no_skill, no_skill], color='orange', linestyle='--')
        for i in result_table.index:
            ax2.plot(result_table_pr.loc[i]['recall'],
                     result_table_pr.loc[i]['precision'],
                     label="{}, pr_auc_score={:.3f}".format(result_table_pr.loc[i]['classifiers'],
                                                         result_table_pr.loc[i]['auc']))

        ax2.legend(loc='upper right')
        ax2.set_title('Precision Recall Curve')
        ax2.axis('tight')
        ax2.set_ylabel('Precision')
        ax2.set_xlabel('Recall')
        ax2.set_xlim(0,1)
        ax2.set_ylim(0,1)
        plt.show()

        return chosen_model
    

    ############## to show metric comparison report ##############
    def metric_comparison_report(self):
        """
        function to show comparison table for chosen iteration of each algorithm

        Input Parameters
        ----------
          final_model : String
            mode value for final model. e.g ctb = Catboost, lgb = LightGBM , xgb = XGBoost

        """

        ksmetric_all = """ """
        if 'ctb' in self.mode:
            ksmetric_ctb = self.ctbmodel.final_ksmetric[["Dev", "OOT(tst)", "Diff", "Bad_Rate", "Best_N_Tree"]]
            ksmetric_ctb = ksmetric_ctb.to_html()
            ksmetric_all = ksmetric_all + "<h3>Catboost</h3>" + ksmetric_ctb

        if 'lgb' in self.mode:
            ksmetric_lgb = self.lgbmodel.final_ksmetric[["Dev", "OOT(tst)", "Diff", "Bad_Rate", "Best_N_Tree"]]
            ksmetric_lgb = ksmetric_lgb.to_html()
            ksmetric_all = ksmetric_all + "<h3>LightGBM</h3>" + ksmetric_lgb

        if 'xgb' in self.mode:
            ksmetric_xgb = self.xgbmodel.final_ksmetric[["Dev", "OOT(tst)", "Diff", "Bad_Rate", "Best_N_Tree"]]
            ksmetric_xgb = ksmetric_xgb.to_html()
            ksmetric_all = ksmetric_all + "<h3>XGBoost</h3>" + ksmetric_xgb

        output = f'''
        <h2>Following are the performance metric of the chosen(best model) iteration for each algorithm :-</h2>
          <p>{ksmetric_all}</p> 
          <br>
          '''
        
        display(HTML(output))
        # display(HTML(output))
    # ------------------------------------------------------------------
    # FINAL MODEL
    # ------------------------------------------------------------------
    def fit(self, final_model):
        self.final_mode = final_model

        if final_model == 'lgb':
            self.finalmodel = self.lgbmodel.final_model
            self.finalalgo = self.lgbmodel

        elif final_model == 'ctb':
            self.finalmodel = self.ctbmodel.final_model
            self.finalalgo = self.ctbmodel

        elif final_model == 'xgb':
            self.finalmodel = self.xgbmodel.final_model
            self.finalalgo = self.xgbmodel

        else:
            raise ValueError(f"Unknown final model: {final_model}")


    # ------------------------------------------------------------------
    # MLflow LOGGING
    # ------------------------------------------------------------------
    def log_this_iteration(self, artifact_dir=None):
        mlflow.end_run()
        with mlflow.start_run():
            if artifact_dir is None:
                artifact_dir = tempfile.mkdtemp(prefix="mlflow_artifacts_")

            mlflow.log_param("final_model", self.final_mode)
            mlflow.log_metric("auc_train", roc_auc_score(
                self.df_train[self.target],
                self.finalmodel.predict_proba(self.df_train[self.X])[:, 1]
            ))

            model_path = os.path.join(artifact_dir, "model.pkl")
            with open(model_path, "wb") as f:
                pickle.dump(self.finalmodel, f)

            mlflow.log_artifact(model_path)
            run_id = mlflow.active_run().info.run_id
            return run_id

    # ------------------------------------------------------------------
    # SCORING
    # ------------------------------------------------------------------
    def score(self, df):
        df = df.copy()
        df['proba_bad'] = self.finalmodel.predict_proba(df[self.X])[:, 1]
        df['model_score'] = df['proba_bad'].apply(
            lambda x: int(np.log((1 - x) / x) * self.pdo / np.log(2) + self.basepoints)
        )
        return df
    
    ############## initialize ks_search class ##############
    def __mod_ks(self, train, test):
        """
        Private method to initialize the ks_searh class to a class attribute 'ks'

        Input Parameters
        ----------
          train : pandas dataframe
            training dataset which is used to train the model
          test : pandas dataframe
            test dataset which is used to evaluate the model's performance

        """
        self.ks = ks_search(train[self.X], train[self.target],
                            train[self.weight],
                            test[self.X], test[self.target],
                            test[self.weight])

        self.train_decile, self.test_decile, self.perf_metric = self.ks.result_ks(self.finalmodel)

        return True
    
    
    ############## to do some clean up of data for HTML reports ##############
    def __fit_cleanup(self):
        """
        Private method to do some clean up of data for HTML reports
        """
        # summary for all the available variables in input dataframe
        self.summary_df_all = pd.DataFrame({'Column': self.df_pandas.columns.tolist(),
                                            'Data_Type': self.df_pandas.dtypes.tolist(),
                                            'Unique_Values': self.df_pandas.nunique().tolist(),
                                            # 'Total_Count': self.df_pandas.count().tolist(),
                                            # 'NULL_Count': self.df_pandas.isnull().sum().tolist(),
                                            'NULL_Prnct': ((self.df_pandas.isnull().sum() / self.df_pandas.shape[
                                                0]) * 100).tolist()})

        self.summary_df_all['Coverage_Percent'] = 100 - self.summary_df_all['NULL_Prnct']

        self.summary_df_all = self.summary_df_all.sort_values(by='Coverage_Percent', ascending=False).reset_index(
            drop=True)

        # summary for model variables in input dataframe
        df_model_vars = self.df_pandas[self.model_vars]
        self.summary_df = pd.DataFrame({'Column': df_model_vars.columns.tolist(),
                                        'Data_Type': df_model_vars.dtypes.tolist(),
                                        'Unique_Values': df_model_vars.nunique().tolist(),
                                        # 'Total_Count': df_model_vars.count().tolist(),
                                        # 'NULL_Count': df_model_vars.isnull().sum().tolist(),
                                        'NULL_Prnct': ((df_model_vars.isnull().sum() / df_model_vars.shape[
                                            0]) * 100).tolist()})

        self.summary_df['Coverage_Percent'] = 100 - self.summary_df['NULL_Prnct']

        self.summary_df = self.summary_df.sort_values(by='Coverage_Percent', ascending=False).reset_index(drop=True)

        # ----- clean up train results
        inp_list = ['min_scr', 'max_scr', 'total', 'bads', 'goods', 'bad_rate',
                    'cumm_bad', 'cumm_good', 'KS', 'max_ks', 'gini', 'auc']

        df = self.ks.res_comp[1][inp_list]
        df.index = range(1, len(df) + 1)
        df = df.reset_index()
        df.rename(
            columns={'index': 'Score', 'bads': 'Targets', 'goods': 'NonTargets', 'bad_rate': 'TargetRate', 'KS': 'KS_o',
                     'cumm_bad': 'CumTargetCapRate_o',
                     'cumm_good': 'CumNonTargetCapRate_o'}, inplace=True)

        df['CumTargetCapRate'] = df['CumTargetCapRate_o'].map(lambda x: '{:,.2%}'.format(x / 100))
        df['CumNonTargetCapRate'] = df['CumNonTargetCapRate_o'].map(lambda x: '{:,.2%}'.format(x / 100))
        df['KS'] = df['KS_o'].map(lambda x: '{:,.2f}'.format(x))
        self.train_result = df[['Score', 'min_scr', 'max_scr', 'total', 'Targets', 'NonTargets', 'TargetRate',
                                'CumTargetCapRate', 'CumNonTargetCapRate', 'KS', 'max_ks']]

        # ----- clean up test results
        df = self.ks.res_comp[2][inp_list]
        df.index = range(1, len(df) + 1)
        df = df.reset_index()
        df.rename(
            columns={'index': 'Score', 'bads': 'Targets', 'goods': 'NonTargets', 'bad_rate': 'TargetRate', 'KS': 'KS_o',
                     'cumm_bad': 'CumTargetCapRate_o',
                     'cumm_good': 'CumNonTargetCapRate_o'}, inplace=True)

        df['CumTargetCapRate'] = df['CumTargetCapRate_o'].map(lambda x: '{:,.2%}'.format(x / 100))
        df['CumNonTargetCapRate'] = df['CumNonTargetCapRate_o'].map(lambda x: '{:,.2%}'.format(x / 100))
        df['KS'] = df['KS_o'].map(lambda x: '{:,.2f}'.format(x))
        self.test_result = df[['Score', 'min_scr', 'max_scr', 'total', 'Targets', 'NonTargets', 'TargetRate',
                               'CumTargetCapRate', 'CumNonTargetCapRate', 'KS', 'max_ks']]

        return True
    
    ############## to produce artifacts required for HTML reports ##############
    def enable_report(self):
        """
        method to produce artifacts required for HTML reports
        """
        self.__mod_ks(self.df_train, self.df_test)

        self.__fit_cleanup()

        self.df_pandas['probability'] = self.finalmodel.predict_proba(self.df_pandas[self.X])[:, 1]
        # compare model against other desired metrics
        self.comparison = self.ks.score_comp(self.df_pandas, self.target, self.weight, self.comp_against)

        return True
    
    ############## to generate the EDA report and summary of model variables ##############
    def generate_report1(self):
        """
        Function to generate the EDA report and summary of model variables.
        This report is not valid if user has not run the function eda_fit_feature_selection() from this class
        """
        if self.flag_eda_required is True:
            output = "[INFO] 'model.eda_fit_feature_selection()' was not called by user. So this report cannot be generated."
            print(output)
            self.report1_html = output
        else:
            # self.test_size = self.param_dict['test_size']

            # waterfall df
            data = {'Step': ['Total Number of variables at preprocessing',
                             'Total number of variables identified as duns, date or present in drop_list',
                             'Total Number of variables identified by Missing',
                             'Total Number of variables identified as Single unique',
                             'Total Number of variables removed using variable clustering',
                             'Total Number of Variables (Numeric) with correlation greater than correlation_threshold',
                             'Total Number of variables with vif value greater than threshold',
                             'Total Number of Variables (Categorical) with correlation greater than correlation_threshold',
                             'Total Number of variables with cumulative importance(LightGBM) less than 0.8'
                             ],
                    'Records':
                        [self.df_pandas.shape[1],
                         len(self.drop_reason['duns_date_droplist']),
                         len(self.drop_reason['missing']),
                         len(self.drop_reason['single_unique']),
                         len(self.drop_reason['var_clustering']),
                         len(self.drop_reason['collinear']),
                         len(self.drop_reason['vif_drop']),
                         len(self.drop_reason['collinear_catg']),
                         len(self.drop_reason['Lightgbm_Imprtnc_drop'])
                         ]
                    }
            waterfall_df = pd.DataFrame.from_dict(data)

            if self.flag_train_test_split is True:
                strng_train_test_info = f"Development sample was split into Train {(1 - self.test_size) * 100}%. We ran Machine Learning Models and selected the model that had the best fit to the sample data."
            else:
                strng_train_test_info = f"Training and Test dataset were provided separately. We ran Machine Learning Models and selected the model that had the best fit to the sample data."

            output = f''' 
    <h2 id="dun-bradstreet-automated-scorecard">Dun &amp; Bradstreet , Automated ML Model</h2>
    <p>D&amp;B Automated Machine Learning is one of a kind package enabling modelers develop Machine Learning model in an automated manner. </p>
    <h3 id="objective">Objective</h3>
    <p> AutoML model</p>
    <h3 id="data-processing-and-bad-rates">Data processing and Bad Rates</h3>
    <p>Number of Columns : {self.df_pandas.shape[1]}</p>
    <p>Number of Rows : {self.df_pandas.shape[0]} </p>
    <p>Bad Rate :- {str(np.round(self.df_pandas[self.target].mean() * 100, 2)) + '%'} </p>
    <h3 id="exploratory-data-analysis">Exploratory Data Analysis</h3>
    <p>The exploratory analysis was done to identify strong predictors for the target value. </p>

    <!-- <p>An Exploratory Data Analysis was conducted to examine the predictive power of attributes ,
    </p> -->
    <p><strong>Variable drop description</strong> : We use multiple technique to drop the variables , 
    enabling us to select the appropriate variables. </p>
    <p>Following table represents the drop on the variable :- </p>
    {waterfall_df.to_html(index=False)}

    <h3 id="model-results">MODEL RESULTS</h3>
    <p> {strng_train_test_info} </p>
    <p>The selected model used the following <strong>{len(self.X)} variables as predictors</strong>:</p>
    <p><pre>{self.X}</pre> </p>
    <p>Here is the summary of model variables : </p>
    <p>{self.summary_df.to_html(index=False)} </p>
    <p>The model has been built on the above variables. </p>
    <p>Summary of all the variables can be accessed by calling 'model.summary_df_all': </p>
    '''

            self.report1_html = output

            display(HTML(output))
            # display(HTML(output))

    ############## to generate the model performance report ##############
    def generate_report2(self):
        """
        Function to generate the model performance report.
        """

        # clean up feature importance
        if self.final_mode == 'xgb':
            features = pd.DataFrame({"importance": self.finalmodel.get_booster().get_score(importance_type='gain')},
                                    index=self.X).reset_index().sort_values(by='importance', ascending=False)
        elif self.final_mode == 'ctb':
            # Get feature importance based on the default method (which uses Gain under the hood)
            features = pd.DataFrame({"importance": self.finalmodel.get_feature_importance(type='FeatureImportance')},
                                    index=self.X).reset_index().sort_values(by='importance', ascending=False)
        else:
            features = pd.DataFrame({"importance": self.finalmodel.booster_.feature_importance(importance_type="gain")},
                                    index=self.X).reset_index().sort_values(by='importance', ascending=False)

        features.columns = ['Variable', 'Importance']
        features['Importance_pcnt'] = features['Importance']/features['Importance'].sum()
        features['Importance_pcnt'] = features['Importance_pcnt'].map(lambda x: '{:,.2%}'.format(x))

        output = f'''

        <p>The following is the <strong> best model parameter based on the AutoML</strong> :- </p>
        <pre>{self.finalmodel}</pre> 
        <br>
        <p> The Feature importance are the following :- 

        {features.to_html(index=False)}

        <p>From the above variable list , The <strong>following is the performance summary</strong> :- </p>
        <p>

        {self.ks.res_comp[0].rename(
                    columns={'Development': 'Training', 'Out_of_time': 'Validation'},
                    index={'10%': 'capture_rate_10%', '20%': 'capture_rate_20%'}).T.to_html()}

        </p>
        <p><strong>Performance Metrics Summary</strong></p>
        <p><strong>ROC/AUC</strong></p>
        <p>Area under ROC curve is used as a measure of quality of the classification models. An area under the ROC curve of 0.8, for example, means that a randomly selected case from the group with the target equals 1 has a score larger than that for a randomly chosen case from the group with the target equals 0 in 80% of the time. When a classifier cannot distinguish between the two groups, the area will be equal to 0.5 (the ROC curve will coincide with the diagonal). When there is a perfect separation of the two groups, i.e., no overlapping of the distributions, the area under the ROC curve reaches to 1 (the ROC curve will reach the upper left corner of the plot).</p>
        <p><strong>GINI</strong></p>
        <p>The Gini Coefficient or Gini Index measures the inequality among the values of a variable. Higher the value of an index, more dispersed is the data. Alternatively, the Gini coefficient can also be calculated as the half of the relative mean absolute difference.</p>
        <p><strong>KS</strong></p>
        <p>A tool to compare model performance, K-S is a measure of the degree of separation between the positive and negative distributions. The K-S is 100 if the scores partition the population into two separate groups in which one group contains all the positives and the other all the negatives. On the other hand, If the model cannot differentiate between positives and negatives, then it is as if the model selects cases randomly from the population. The higher the value the better the model is at separating the positive from negative cases.</p>
        <p>Following is the <strong>performance on training set</strong> :- </p>

        <p>{self.train_result.to_html(index=False)}</p>

        <p>Following is the <strong>performance on testing set</strong> :- </p>

        <p>{self.test_result.to_html(index=False)}</p>

        <h3 id="comparison-against-other-scores">Model Performance Compared to Standard Scores</h3>
        <p>The following set of scores/prob were used for comparison :- </p>

        <p><strong>
        {list(self.comp_against.keys())}
        </strong> </p>

        <p>Here&#39;s the comparison on full dataset:- </p>
        <p>{self.comparison.to_html()}</p>
        '''

        self.report2_html = output

        display(HTML(output))
        # display(HTML(output))

     # fig is a matplotlib.figure.Figure object
    def display2(self, fig):
        plt.show()
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        data = buf.read()
        image = b64encode(data).decode("utf-8")
        buf.close()
        self.lift_and_gain_charts = f"""<img src="data:image/png;base64,{image}"></img>"""
        display(HTML(self.lift_and_gain_charts))
        # display(HTML(self.lift_and_gain_charts))

        return True

    ############## to get the list of plots ##############
    def __get_list_plot(self):
        """
        Private method to get the list of plots to be plotted
        """
        out_df = pd.DataFrame.from_dict({'Random guess': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]}, orient='columns')
        lres = []
        d1 = {'Random guess': 0}
        _ks = ks_search(self.df_train[self.X], self.df_train[self.target],
                        self.df_train[self.weight],
                        self.df_test[self.X], self.df_test[self.target],
                        self.df_test[self.weight])
        for i, j in self.comp_against.items():
            df1 = self.df_pandas[~self.df_pandas[i].isnull()]
            out_df[i] = _ks.KS_train(df1[self.target], df1[i], df1[self.weight], bins=10, mode=j)['cumm_bad']
            lres.append(i)
            d1[i] = 0
        out_df = pd.concat([pd.DataFrame(d1, index=[0]), out_df], ignore_index=True)
        out_df['index'] = out_df['Random guess']
        out_df.set_index('index', inplace=True)
        out_df1 = out_df.copy()
        for i in self.comp_against.keys():
            out_df1[i] = out_df1[i] / out_df['Random guess']
        out_df1.drop(['Random guess'], axis=1, inplace=True)
        return out_df, out_df1

    ############## to generate performance charts ##############
    def generate_charts(self, figsize=(12, 5)):
        """
        Generate performance charts of model
        """
        # ---- get the list of dataframes to be plotted
        out_df2, out_df1 = self.__get_list_plot()

        lines = [':']
        for i in range(0, out_df1.shape[1] - 1):
            lines.append('-')

        #### Generate Lift Chart
        # It is a representation of the improvement that a model provides compared against a random guess.
        # A comparison between lift charts for different models gives an idea about their performance when
        # tested on the same dataset. It is calculated as the ratio between the results obtained with and without the model.
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize)
        out_df1.plot(ax=ax1, marker='o', title='Lift Chart : Comparison', grid=True)
        ax1.set_ylabel('Lift')
        ax1.set_xlabel('Decile')

        #### Generate Gain Chart
        # Gain at a given decile level is the ratio of cumulative number of targets (events) up to that decile
        # to the total number of targets (events) in the entire data set. A gain chart shows Predicted Positive Rate
        # (or support of the classifier) vs True Positive Rate (or sensitivity of the classifier). It says how much
        # population we should sample to get the desired sensitivity of our classifier.
        out_df2.plot(ax=ax2, marker='o', title='Gains Chart : Comparison', grid=True, style=lines)
        ax2.set_ylabel('Capture Rate')
        ax2.set_xlabel('Decile')

        # fig.savefig("lift_and_gain_charts.png")
        # self.lift_and_gain_charts = plt.imread("lift_and_gain_charts.png")

        self.display2(fig)



    def gen_summary_tab(self, df, var, target_label, sort_ascending=True, full=True):

        df = df.copy()

        # df['large_fund'] = 0
        # df.loc[df['fund_amt'] >= 25000, 'large_fund'] = 1

        #################################################
        # decide measure list
        #################################################
        measure_dict = {
            'flg_resp': ['count', 'sum']
            # ,
            # 'flg_sub': ['sum'],
            # 'flg_appr': ['sum'],
            # 'flg_fund': ['sum'],
            # 'fund_amt': ['sum'],
            # 'large_fund': ['sum']
        }

        if any('score_RESP' in c for c in df.columns):
            measure_dict['score_RESP'] = ['min', 'max']
        if any('bestmodel_score' in c for c in df.columns):
            measure_dict['bestmodel_score'] = ['min', 'max']

        #################################################
        # group by
        #################################################
        summary = df.groupby(var, dropna=False).agg(measure_dict)
        summary.sort_index(inplace=True, ascending=sort_ascending)
        summary = summary.astype(float)

        summary['cnt'] = summary['flg_resp']['count']
        summary['cumu_cnt'] = summary['cnt'].cumsum()

        resp_sum = summary['flg_resp']['sum']
        resp_cnt = summary['flg_resp']['count']

        summary['cumu_resp'] = resp_sum.cumsum()
        summary['cumu_resp_pcnt'] = resp_sum.cumsum() / resp_sum.sum()

        # summary['cumu_sub'] = summary['flg_sub']['sum'].cumsum()
        # summary['cumu_sub_pcnt'] = summary['flg_sub']['sum'].cumsum() / summary['flg_sub']['sum'].sum()

        # summary['cumu_appr'] = summary['flg_appr']['sum'].cumsum()
        # summary['cumu_appr_pcnt'] = summary['flg_appr']['sum'].cumsum() / summary['flg_appr']['sum'].sum()

        # summary['cumu_fund'] = summary['flg_fund']['sum'].cumsum()
        # summary['cumu_fund_pcnt'] = summary['flg_fund']['sum'].cumsum() / summary['flg_fund']['sum'].sum()

        # summary['large_funded'] = summary['large_fund']['sum']
        # summary['cumu_large_funded'] = summary['large_fund']['sum'].cumsum()
        # summary['cumu_large_funded_pcnt'] = (
        #     summary['large_fund']['sum'].cumsum() /
        #     summary['large_fund']['sum'].sum()
        # )

        #################################################
        # rates (protected divisions)
        #################################################
        denom_resp = resp_cnt.replace(0, np.nan)
        # denom_sub = summary['flg_sub']['sum'].replace(0, np.nan)
        # denom_appr = summary['flg_appr']['sum'].replace(0, np.nan)

        summary['resp_rate'] = (100 * resp_sum / denom_resp).round(2)
        # summary['sub_rate'] = (100 * summary['flg_sub']['sum'] / denom_resp).round(2)
        # summary['appr_rate'] = (100 * summary['flg_appr']['sum'] / denom_resp).round(2)
        # summary['fund_rate'] = (100 * summary['flg_fund']['sum'] / denom_resp).round(2)

        # summary['resp_sub_rate'] = (100 * summary['flg_sub']['sum'] / resp_sum.replace(0, np.nan)).round(2)
        # summary['sub_appr_rate'] = (100 * summary['flg_appr']['sum'] / denom_sub).round(2)
        # summary['appr_fund_rate'] = (100 * summary['flg_fund']['sum'] / denom_appr).round(2)

        # summary['avg_funded'] = (
        #     summary['fund_amt']['sum'] /
        #     summary['flg_fund']['sum'].replace(0, np.nan)
        # ).round(0)

        # summary['cumu_fund_amt'] = summary['fund_amt']['sum'].cumsum()
        # summary['cumu_fund_amt_pcnt'] = (
        #     summary['fund_amt']['sum'].cumsum() /
        #     summary['fund_amt']['sum'].sum()
        # )

        #################################################
        # good / bad ratios
        #################################################
        summary['good_bad_ratio_resp'] = resp_sum / (resp_cnt - resp_sum).replace(0, np.nan)
        # summary['good_bad_ratio_sub'] = summary['flg_sub']['sum'] / (resp_cnt - summary['flg_sub']['sum']).replace(0, np.nan)
        # summary['good_bad_ratio_appr'] = summary['flg_appr']['sum'] / (resp_cnt - summary['flg_appr']['sum']).replace(0, np.nan)
        # summary['good_bad_ratio_fund'] = summary['flg_fund']['sum'] / (resp_cnt - summary['flg_fund']['sum']).replace(0, np.nan)

        #################################################
        # prepare reduced view
        #################################################
        target_short = target_label[4:]

        list_measure_when_not_full = {
            'cumu_cnt',
            f'flg_{target_short}',
            'resp_rate',
            # 'sub_rate',
            # 'appr_rate',
            f'cumu_{target_short}',
            f'cumu_{target_short}_pcnt',
            f'good_bad_ratio_{target_short}'
            #,
            # 'resp_sub_rate',
            # 'sub_appr_rate',
            # 'appr_fund_rate',
            # 'avg_funded',
            # 'cumu_fund_pcnt',
            # 'cumu_fund_amt_pcnt'
        }

        if 'bestmodel_score' in df.columns:
            summary['min_bestmodel_score'] = summary['bestmodel_score']['min']
            summary['max_bestmodel_score'] = summary['bestmodel_score']['max']
            list_measure_when_not_full.add('min_bestmodel_score')
            list_measure_when_not_full.add('max_bestmodel_score')

        #################################################
        # output
        #################################################
        if full:
            return summary
        else:
            return summary[list_measure_when_not_full]
        
 
    def gen_evaluation_metrics_new(self, dff, score_col_name, metric,
                                    optimize_threshold=True, min_precision=None):

        dff[metric] = pd.to_numeric(dff[metric], errors='coerce')
        dff['bestmodel_score'] = dff[score_col_name]

        # default threshold = 0.5
        dff['model_pred'] = 0
        dff.loc[dff['bestmodel_score'] >= 0.5, 'model_pred'] = 1

        score_bins = np.arange(0, 1.1, 0.1)
        dff['bestmodel_scorebin'] = pd.cut(
            dff['bestmodel_score'], bins=score_bins, right=False
        )
        dff['bestmodel_decile'] = (
            pd.qcut(dff['bestmodel_score'], q=10, duplicates='drop')
            .rank(method='dense', ascending=False)
        )

        # dff['large_fund'] = 0
        # dff.loc[dff['fund_amt'] >= 25000, 'large_fund'] = 1

        # ---- KDE plot ----
        g = sns.FacetGrid(dff, hue=metric, palette=['red', 'green'])
        g.map(sns.kdeplot, "bestmodel_score")
        plt.title(f"Score distribution by {metric}")
        plt.show()

        # ---- Metrics ----
        acc = accuracy_score(dff[metric], dff['model_pred'])
        prec_val = precision_score(dff[metric], dff['model_pred'])
        rec_val = recall_score(dff[metric], dff['model_pred'])
        f1_val = f1_score(dff[metric], dff['model_pred'])
        roc_auc_val = roc_auc_score(dff[metric], dff['bestmodel_score'])

        print(f"roc_auc: {roc_auc_val:.3f}")
        print(f"accuracy: {acc:.3f}")
        print(f"precision: {prec_val:.3f}")
        print(f"recall: {rec_val:.3f}")
        print(f"f1_score: {f1_val:.3f}")

        # ---- Capture rates ----
        df_sorted = dff.sort_values(
            by='bestmodel_score', ascending=False
        ).reset_index(drop=True)

        total_positives = df_sorted[metric].sum()

        top_10 = df_sorted.head(int(len(df_sorted) * 0.10))
        top_20 = df_sorted.head(int(len(df_sorted) * 0.20))

        capture_rate_10 = (
            top_10[metric].sum() / total_positives
            if total_positives > 0 else np.nan
        )
        capture_rate_20 = (
            top_20[metric].sum() / total_positives
            if total_positives > 0 else np.nan
        )

        print(f"capture_rate_10%: {capture_rate_10:.3f}")
        print(f"capture_rate_20%: {capture_rate_20:.3f}")

        # enforce int types
        for col in ['flg_resp']: #'flg_sub']: , 'flg_appr', 'flg_fund', 'fund_amt']
            dff[col] = dff[col].astype(int)

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        scorebin_df = self.gen_summary_tab(
            df=dff,
            var='bestmodel_scorebin',
            target_label=metric,
            sort_ascending=False,
            full=True
        )

        decile_df = self.gen_summary_tab(
            df=dff,
            var='bestmodel_decile', 
            target_label=metric,
            sort_ascending=True,
            full=True
        )

        # Convert Interval columns to string for HTML
        for col in scorebin_df.select_dtypes(include=['interval']).columns:
            scorebin_df[col] = scorebin_df[col].astype(str)

        for col in decile_df.select_dtypes(include=['interval']).columns:
            decile_df[col] = decile_df[col].astype(str)

        # ---- Display (Python-safe) ----
        display(HTML(scorebin_df.to_html()))
        display(HTML(decile_df.to_html()))
def show_shap_and_pdp(
    model,
    df,
    feature_cols,
    max_display=35,
    pdp_n_cols=2
):
    """
    Generate SHAP feature importance, summary plots,
    and partial dependence plots for tree-based models.

    Parameters
    ----------
    model : fitted model
        Trained tree-based model (XGB, LGBM, CatBoost)
    df : pd.DataFrame
        Scoring dataframe
    feature_cols : list
        List of model feature names
    max_display : int, optional
        Number of features to display in SHAP summary plots
    pdp_n_cols : int, optional
        Number of columns for PDP plots
    """

  

    # --------------------------------------------------
    # Ensure categorical dtypes (important for trees)
    # --------------------------------------------------
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == "object" or df[c].dtype.name == "category":
            df[c] = df[c].astype("category")

    # --------------------------------------------------
    # SHAP Explainer
    # --------------------------------------------------
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(df[feature_cols])

    # --------------------------------------------------
    # Handle binary vs multiclass SHAP outputs
    # --------------------------------------------------
    if isinstance(shap_values, list):
        shap_vals_for_importance = shap_values[1]
    else:
        shap_vals_for_importance = shap_values

    # --------------------------------------------------
    # Feature importance (mean |SHAP|)
    # --------------------------------------------------
    vals = np.abs(shap_vals_for_importance).mean(axis=0)
    feature_importance = pd.DataFrame({
        "col_name": feature_cols,
        "feature_importance_vals": vals
    }).sort_values("feature_importance_vals", ascending=False)

    print("\n[INFO] SHAP Feature Importance")
    display(feature_importance)

    # --------------------------------------------------
    # SHAP bar summary
    # --------------------------------------------------
    shap.summary_plot(
        shap_vals_for_importance,
        df[feature_cols],
        plot_type="bar",
        max_display=max_display
    )

    # --------------------------------------------------
    # SHAP beeswarm
    # --------------------------------------------------
    shap.summary_plot(
        shap_vals_for_importance,
        df[feature_cols],
        max_display=max_display,
        plot_size=(20, 9)
    )

    # --------------------------------------------------
    # Partial Dependence Plots (top features)
    # --------------------------------------------------
    num_plots = len(feature_importance)
    n_rows = (num_plots + pdp_n_cols - 1) // pdp_n_cols

    plt.figure(figsize=(18, 4 * n_rows))

    for i, colname in enumerate(feature_importance["col_name"]):
        cutoff = df[colname].quantile(0.95)

        shap.plots.partial_dependence(
            colname,
            model.predict,
            df[feature_cols],
            ice=False,
            model_expected_value=True,
            feature_expected_value=True,
            xmax=cutoff
        )

    plt.tight_layout()
    plt.show()

    return feature_importance


