# ============================================================
# Feature Engineering + Model Training (Portable Python)
# ============================================================

import inspect
import numpy as np
import pandas as pd

# -------------------- Stats / SciPy -------------------------
import scipy.stats as ss
from scipy.stats import spearmanr
from scipy.cluster import hierarchy

# -------------------- ML / Models ---------------------------
import lightgbm as lgb
import catboost as ctb
import xgboost as xgb
from xgboost import callback

from sklearn.model_selection import (
    StratifiedKFold,
    cross_val_score
)
from sklearn.metrics import (
    roc_auc_score,
    roc_curve,
    auc
)
from statsmodels.stats.outliers_influence import variance_inflation_factor

# -------------------- Hyperopt -------------------------------
from hyperopt import (
    fmin,
    tpe,
    hp,
    Trials,
    STATUS_OK
)

# ============================================================
# FEATURE ENGINEERING
# ============================================================

class feature_engg:
    """
    Feature engineering and selection class
    """

    def __init__(
        self,
        data,
        target,
        weight,
        missing_threshold,
        correlation_threshold,
        vif_threshold,
        fixed_vars,
        exclusion_list
    ):
        self.data = data
        self.target = target
        self.weight = weight
        self.missing_threshold = missing_threshold
        self.correlation_threshold = correlation_threshold
        self.vif_threshold = vif_threshold
        self.fixed_vars = fixed_vars
        self.exclusion_list = exclusion_list
        self.drop_reason = {}

    # --------------------------------------------------------
    # Missing values
    # --------------------------------------------------------
    def identify_missing(self):
        df_miss = pd.DataFrame({
            "Column": self.data.columns,
            "Null_prnct": self.data.isnull().mean()
        })
        return df_miss[df_miss["Null_prnct"] > self.missing_threshold]["Column"].tolist()

    # --------------------------------------------------------
    # Single unique
    # --------------------------------------------------------
    def identify_single_unique(self):
        df_uniq = pd.DataFrame({
            "Column": self.data.columns,
            "Distinct_Values": self.data.nunique()
        })
        return df_uniq[df_uniq["Distinct_Values"] == 1]["Column"].tolist()

    # --------------------------------------------------------
    # Variable clustering (SciPy)
    # --------------------------------------------------------
    def varclus_scipy(self, df, max_num_clusters=100):
        corr = spearmanr(df).correlation
        corr_linkage = hierarchy.ward(corr)

        n_min, n_max = df.shape[1] // 6, df.shape[1] // 5
        if n_min > max_num_clusters:
            n_min, n_max = int(max_num_clusters * 0.9), int(max_num_clusters * 1.1)

        thr, thr_above = 0.1, 1
        while True:
            cluster_ids = hierarchy.fcluster(corr_linkage, thr, criterion="distance")
            if len(np.unique(cluster_ids)) > n_max:
                thr, thr_above = thr * 2, thr
            elif len(np.unique(cluster_ids)) < n_min:
                thr = (thr + thr_above) / 2
            else:
                break

        return pd.DataFrame({
            "variable": df.columns,
            "cluster": cluster_ids
        }).sort_values("cluster")

    # --------------------------------------------------------
    # Feature importance (LightGBM)
    # --------------------------------------------------------
    def get_feature_importance(self, df):
        cols_numeric = df.select_dtypes(include=np.number).columns.tolist()
        cols_numeric.remove(self.target)
        cols_catg = [c for c in df.columns if c not in cols_numeric and c != self.target]

        for col in cols_catg:
            df[col] = df[col].astype("category")

        model = lgb.LGBMClassifier()
        model.fit(df.drop(columns=[self.target]), df[self.target], categorical_feature=cols_catg)

        return (
            pd.DataFrame({
                "variable": df.drop(columns=[self.target]).columns,
                "Importance": model.feature_importances_
            })
            .sort_values("Importance", ascending=False)
        )

    # --------------------------------------------------------
    # VIF
    # --------------------------------------------------------
    def __calc_vif(self, df):
        return pd.DataFrame({
            "variables": df.columns,
            "VIF": [variance_inflation_factor(df.values, i) for i in range(df.shape[1])]
        })

    def vif(self, df):
        df = df.select_dtypes(include=np.number)
        df = df[[c for c in df.columns if c not in self.fixed_vars]].fillna(0)

        vif_df = self.__calc_vif(df).sort_values("VIF", ascending=False)
        drop_list = vif_df[vif_df["VIF"] > self.vif_threshold]["variables"].tolist()

        return drop_list, vif_df

    # --------------------------------------------------------
    # Numeric correlation
    # --------------------------------------------------------
    def identify_collinear_numeric(self, threshold):
        cols_numeric = self.data.select_dtypes(include=np.number).columns.tolist()
        cols_numeric.remove(self.target)

        corr = self.data[cols_numeric].corr()
        upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))

        return [
            col for col in upper.columns
            if any(upper[col].abs() > threshold)
        ]

    # --------------------------------------------------------
    # Run pipeline
    # --------------------------------------------------------
    def run_feature_engg(self):

        self.data = self.data[[c for c in self.data.columns if c not in self.exclusion_list]]

        miss = [c for c in self.identify_missing() if c not in self.fixed_vars]
        self.drop_reason["missing"] = miss
        self.data = self.data.drop(columns=miss)

        uniq = [c for c in self.identify_single_unique() if c not in self.fixed_vars]
        self.drop_reason["single_unique"] = uniq
        self.data = self.data.drop(columns=uniq)

        return self.data.columns


# ============================================================
# KS SEARCH + MODEL EVALUATION
# ============================================================

class ks_search:

    def __init__(self, dev1, bad_dev, weight_bad, oot1, bad_oot, weight_oot):
        self.dev1 = dev1
        self.bad_dev = bad_dev
        self.weight_bad = weight_bad
        self.oot1 = oot1
        self.bad_oot = bad_oot
        self.weight_oot = weight_oot
        self.result_out = None
        self.param = None

    def KS_train(self, y_train, y_train_rf, wt, bins=10, mode=None):

        data = pd.DataFrame({
            "bad": y_train,
            "Prob_bad": y_train_rf,
            "wt": wt
        })

        data["good"] = 1 - data["bad"]

        if mode == "score":
            data = data.sort_values("Prob_bad", ascending=True).reset_index(drop=True)
        else:
            data = data.sort_values("Prob_bad", ascending=False).reset_index(drop=True)

        data["wt_cumsum"] = data["wt"].cumsum()
        data["wt_good"] = np.where(data["bad"] == 0, data["wt"], 0)
        data["wt_bad"] = np.where(data["bad"] == 1, data["wt"], 0)

        data["bucket"] = pd.cut(data["wt_cumsum"], bins)

        grouped = data.groupby("bucket")

        agg1 = pd.DataFrame()
        agg1["Decile"] = list(range(5, 105, int(100 / bins)))
        agg1["min_scr"] = grouped["Prob_bad"].min().values
        agg1["max_scr"] = grouped["Prob_bad"].max().values
        agg1["bads"] = grouped["wt_bad"].sum().values
        agg1["goods"] = grouped["wt_good"].sum().values
        agg1["total"] = agg1["bads"] + agg1["goods"]

        # --- rates & cumulative stats
        agg1["bad_rate"] = (agg1["bads"] / agg1["total"]).apply("{0:.2%}".format)
        agg1["cumm_bad"] = (agg1["bads"] / data["wt_bad"].sum()).cumsum() * 100
        agg1["cumm_good"] = (agg1["goods"] / data["wt_good"].sum()).cumsum() * 100
        agg1["KS"] = np.abs(np.round(agg1["cumm_bad"] - agg1["cumm_good"], 4))

        max_ks = agg1["KS"].max()
        agg1["max_ks"] = agg1["KS"].apply(lambda x: "<----" if x == max_ks else "")

        # --- AUC / GINI
        auc = roc_auc_score(y_train, y_train_rf, sample_weight=wt)
        gini = 2 * auc - 1

        agg1["gini"] = pd.Series([abs(gini)] * len(agg1))
        agg1["auc"]  = pd.Series([(1 - auc if mode == "score" else auc)] * len(agg1))


        # --- Lift / PI
        agg1["lcum_bad"] = agg1["cumm_bad"].shift(1).fillna(0)
        agg1["cum_total"] = ((agg1["total"] / agg1["total"].sum()) * 100).cumsum()
        agg1["lcum_total"] = agg1["cum_total"].shift(1).fillna(0)

        agg1["area"] = 0.5 * (
            (agg1["cumm_bad"] - agg1["cum_total"]) +
            (agg1["lcum_bad"] - agg1["lcum_total"])
        ) * (agg1["cum_total"] - agg1["lcum_total"])

        agg1["cum_area"] = agg1["area"].cumsum()
        agg1["areab"] = 0.5 - ((agg1["bads"].sum() / agg1["total"].sum()) / 2)
        agg1["PI"] = agg1["cum_area"] / agg1["areab"]

        return agg1

    def result_ks(self, model):

        y_dev = model.predict_proba(self.dev1)[:, 1]
        y_oot = model.predict_proba(self.oot1)[:, 1]

        a1 = self.KS_train(self.bad_dev, y_dev, self.weight_bad, bins=10)
        a2 = self.KS_train(self.bad_oot, y_oot, self.weight_oot, bins=10)


        res = pd.DataFrame(index=range(7), columns=["Dev", "OOT(tst)"])

        res.loc[0, "Dev"] = a1["auc"].iloc[0]
        res.loc[1, "Dev"] = a1["gini"].iloc[0]
        res.loc[2, "Dev"] = a1["KS"].max()
        res.loc[3, "Dev"] = a1.query("Decile == 5")["cumm_bad"].iloc[0]
        res.loc[4, "Dev"] = a1.query("Decile == 15")["cumm_bad"].iloc[0]
        res.loc[5, "Dev"] = a1.query("Decile == 5")["max_scr"].iloc[0]
        res.loc[6, "Dev"] = a1.query("Decile == 95")["min_scr"].iloc[0]

        res.loc[0, "OOT(tst)"] = a2["auc"].iloc[0]
        res.loc[1, "OOT(tst)"] = a2["gini"].iloc[0]
        res.loc[2, "OOT(tst)"] = a2["KS"].max()
        res.loc[3, "OOT(tst)"] = a2.query("Decile == 5")["cumm_bad"].iloc[0]
        res.loc[4, "OOT(tst)"] = a2.query("Decile == 15")["cumm_bad"].iloc[0]
        res.loc[5, "OOT(tst)"] = a2.query("Decile == 5")["max_scr"].iloc[0]
        res.loc[6, "OOT(tst)"] = a2.query("Decile == 95")["min_scr"].iloc[0]

        res.index = ["ROC_AUC", "Gini", "KS", "10%", "20%", "max_prob", "min_prob"]

        self.res_comp = (res, a1, a2)
        return a1, a2, res

    def randomsearch_KS(self, model):

        dev, oot, res = self.result_ks(model)

        res["Diff"] = res["Dev"] - res["OOT(tst)"]

        bdrate_chk = (
            dev["bad_rate"].str.replace("%", "").astype(float).is_monotonic_decreasing
            & oot["bad_rate"].str.replace("%", "").astype(float).is_monotonic_decreasing
        )

        dout = res.copy()
        dout["Bad_Rate"] = bdrate_chk
        dout["param_dict"] = str(model.get_params())
        dout["Best_N_Tree"] = model.get_params().get("n_estimators")

        return dout

    
    def score_comp(self, df, bad, wt, comp_against):

        lres = []
        for col, mode in comp_against.items():
            df1 = df[~df[col].isnull()]
            lres.append(self.KS_train(df1[bad], df1[col], df1[wt], bins=10, mode=mode))

        result_comp = pd.DataFrame({
            "KS": [x["KS"].max() for x in lres],
            "auc": [x["auc"].iloc[-1] for x in lres],
            "PI": [x["PI"].iloc[-1] for x in lres],
            "10perc": [x["cumm_bad"].iloc[0] for x in lres],
            "20perc": [x["cumm_bad"].iloc[1] for x in lres],
        }, index=comp_against.items())

        return result_comp

    
    
class mode_catboost_bn:
    """
    df - Takes pandas.DataFrame as input. This DataFrame contains final set of variables (along with Target and weight) selected after feature selection process.
   
    """
    def __init__(self, df, param_dict):
        self.data = df
        self.seed = 25
        self.best = None
        self.output_ctb = []
        # self.__dict__.update(_dict)
        #self.n_parallel_models = param_dict['n_parallel_models']
        self.train = param_dict['train']
        self.test = param_dict['test']
        self.X = param_dict['X']
        self.y = param_dict['y']
        self.target = param_dict['target']
        self.weight = param_dict['weight']
        self.catg_features = param_dict.get('catg_features', [])
        self.n_jobs = param_dict.get('n_jobs', -1)
        self.n_iter = param_dict.get('n_iter', 10)
        self.kfolds = param_dict.get('kfolds', 0)

        
    def fit_multi_iter(self, *args, **kwargs):
        # convert categorical features to string (as required by catboost)
        for ele in self.catg_features:
            self.train[ele] = self.train[ele].astype(str)
            self.test[ele] = self.test[ele].astype(str)
        if hasattr(self, "custom_hyper_param_space"):
            hyper_param_space = self.custom_hyper_param_space
        else:
            hyper_param_space = {
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
        
        trials = Trials()   # single-node
        # trials = SparkTrials(parallelism=self.n_parallel_models)  # multi-node

#         rstate = np.random.RandomState(self.seed)

        best = fmin(
            fn=self.hyperparameter_tuning,
            space=hyper_param_space,
            algo=tpe.suggest,
            max_evals=self.n_iter,  # 100
            trials=trials,
#             rstate=np.random.RandomState(self.seed)
            rstate=np.random.default_rng(self.seed)
        )

        print("\n[INFO] Best set of parameters for Catboost are below:\n", best)
        self.best = best


        print(
            "\n[INFO] Select model based on the KS table , Can be accessed by calling 'model.ctbmodel.output_ctb[<nth_iteration>]'. Once the model is selected pass the iteration number to best_iter_comp() function\n")

    def fit(self, nth_iteration=None):
        if nth_iteration is None:
            print(
                "[INFO] Training final model on Best set of hyper-parameters chosen after Hyperparameter tuning -->\n",
                self.best)
            model_param = self.best

        elif isinstance(nth_iteration, int):
            if nth_iteration <= self.n_iter and nth_iteration > 0:
                # get the dictionary of parameters for final model
                model_param = {}
                for key in self.best.keys():
                    model_param[key] = eval(self.output_ctb[nth_iteration - 1]['param_dict'][0].replace('nan', '1'))[
                        key]
                    
                print(
                    f"[INFO] Training final model on set of hyper-parameters present in iteration number {nth_iteration} -->\n",
                    model_param)
            else:
                raise Exception(
                    "Invalid iteration number. Iteration number must be less than 'n_iter' and greater than 0.")

        else:
            raise Exception("Invalid iteration number. Iteration number must be an integer value")

        # Train final model
        self.final_model = ctb.CatBoostClassifier(**model_param,
                                                  random_seed=self.seed,
                                                  objective='Logloss',
                                                  cat_features=self.catg_features,
                                                  eval_metric='AUC',
                                                  thread_count=self.n_jobs,  # 2
                                                  allow_writing_files=False
                                                  )
        self.final_model.fit(X=self.train[self.X], y=self.train[self.y],
                             sample_weight=self.train[self.weight],
                             eval_set=(self.test[self.X], self.test[self.y]),
                             early_stopping_rounds=20,
                             verbose=False  # 100
                             )

        # calculate performance metrices for final model
        self.rs = ks_search(self.train[self.X], self.train[self.y],
                            self.train[self.weight],
                            self.test[self.X], self.test[self.y],
                            self.test[self.weight])

        self.final_ksmetric = self.rs.randomsearch_KS(model=self.final_model)
        
        self.train_decile, self.test_decile, self.perf_metric = self.rs.result_ks(self.final_model)
        return True

    def hyperparameter_tuning(self, hyper_param_space):
        model_hptune = ctb.CatBoostClassifier(**hyper_param_space,
                                              random_seed=self.seed,
                                              objective='Logloss',
                                              cat_features=self.catg_features,
                                              eval_metric='AUC',
                                              thread_count=self.n_jobs,  # 2
                                              allow_writing_files=False,
                                              verbose=False,
                                              )

        # if kfold is not zero
        if self.kfolds != 0:
            # cross validation score
            skf = StratifiedKFold(n_splits=self.kfolds, random_state=self.seed, shuffle=True)
            score_list = cross_val_score(model_hptune, self.train[self.X], self.train[self.y], cv=skf)
            score = score_list.mean()

        # print("----------------------Training---------------------------")
        model_hptune.fit(X=self.train[self.X], y=self.train[self.y],
                         sample_weight=self.train[self.weight],
                         eval_set=(self.test[self.X], self.test[self.y]),
                         early_stopping_rounds=20,
                         verbose=False  # 100
                         )
        print(hyper_param_space)

        # calculate metric results to show roc_auc curve
        y_prob = model_hptune.predict_proba(self.test[self.X])[:, 1]
        false_positive_rate, true_positive_rate, thresholds = roc_curve(self.test[self.y], y_prob)
        auc_roc = auc(false_positive_rate, true_positive_rate)

        # when kfold cross validation is not run
        try:
            print(f"[LOGS] mean cross validation score: {score}")
        except:
            score = auc_roc

        # y_pred = model_hptune.predict(self.test[self.X])
        # roc_auc = metrics.roc_auc_score(self.test[self.y], y_pred)
        # accuracy = accuracy_score(y_pred, self.test[self.y])

        # calculate different performance metrices such as ROC_AUC, Gini, KS 10%, 20% etc..
        rs = ks_search(self.train[self.X], self.train[self.y],
                       self.train[self.weight],
                       self.test[self.X], self.test[self.y],
                       self.test[self.weight])

        df_output_perf = rs.randomsearch_KS(model=model_hptune)

        ## append output of each iteration to spark custom accumulator
        ## for multi-node operation using spark distributed computing
        # global output_ctb_acc
        # output_ctb_acc += [df_output_perf] 
        
        # for single-node operation
        self.output_ctb.append(df_output_perf)  

        return {"loss": 1 - score, "status": STATUS_OK}  # "loss": 1 - auc_roc


class mode_lightgbm_bn:
    def __init__(self, df, param_dict):
        self.data = df
        self.seed = 25
        self.best = None
        self.output_lgb = []
        # self.__dict__.update(_dict)
        #self.n_parallel_models = param_dict['n_parallel_models']
        self.train = param_dict['train']
        self.test = param_dict['test']
        self.X = param_dict['X']
        self.y = param_dict['y']
        self.target = param_dict['target']
        self.weight = param_dict['weight']
        self.catg_features = param_dict.get('catg_features', [])
        self.n_jobs = param_dict.get('n_jobs', -1)
        self.n_iter = param_dict.get('n_iter', 10)
        self.kfolds = param_dict.get('kfolds', 0)

    def fit_multi_iter(self,*args, **kwargs):
        # convert categorical features to string (as required by catboost)
        for ele in self.catg_features:
            self.train[ele] = self.train[ele].astype("category")
            self.test[ele] = self.test[ele].astype("category")

        if hasattr(self, "custom_hyper_param_space"):
            hyper_param_space = self.custom_hyper_param_space
        else:
            hyper_param_space = {
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
        trials = Trials()   # single-node
        # trials = SparkTrials(parallelism=self.n_parallel_models)  # multi-node

        best = fmin(
            fn=self.hyperparameter_tuning,
            space=hyper_param_space,
            algo=tpe.suggest,
            max_evals=self.n_iter,  # 100
            trials=trials,
            rstate=np.random.default_rng(self.seed)
        )

        print("\n[INFO] Best set of parameters for LightGBM are below:\n", best)
        self.best = best

#         print(
#             "\n[INFO] Select model based on the KS table , Can be accessed by calling 'output_lgb_acc.value[<nth_iteration>]'. Once the model is selected pass the iteration number to fit_the_model function\n")

        print(
            "\n[INFO] Select model based on the KS table , Can be accessed by calling 'model.lgbmodel.output_lgb[<nth_iteration>]'. Once the model is selected pass the iteration number to best_iter_comp() function\n")

    def fit(self, nth_iteration=None):
        if nth_iteration is None:
            self.best['n_estimators'] = int(self.best['n_estimators'])
            self.best['max_depth'] = int(self.best['max_depth'])
            self.best['min_child_samples'] = int(self.best['min_child_samples'])
            print(
                "[INFO] Training final model on Best set of hyper-parameters chosen after Hyperparameter tuning -->\n",
                self.best)
            model_param = self.best

        elif isinstance(nth_iteration, int):
            if nth_iteration <= self.n_iter and nth_iteration > 0:
                # get the dictionary of parameters for final model
                model_param = {}
                for key in self.best.keys():
                    model_param[key] = eval(self.output_lgb[nth_iteration - 1]['param_dict'][0].replace('nan', '1'))[
                        key]
                print(
                    f"[INFO] Training final model on set of hyper-parameters present in iteration number {nth_iteration} -->\n",
                    model_param)
            else:
                raise Exception(
                    "Invalid iteration number. Iteration number must be less than 'n_iter' and greater than 0.")

        else:
            raise Exception("Invalid iteration number. Iteration number must be an integer value")

        # Train final model
        self.final_model = lgb.LGBMClassifier(**model_param,
                                              random_state=self.seed,
                                              bagging_fraction_seed=self.seed,
                                              feature_fraction_seed=self.seed,
                                              # cat_feature = f'name:{self.catg_features}',
                                              bagging_freq=5,
                                              objective='binary',
                                              n_jobs=self.n_jobs  # 2
                                              )
        y_train = self.train[self.y].values.ravel()
        y_test  = self.test[self.y].values.ravel()
        self.final_model.fit(X=self.train[self.X], y=y_train,
                             sample_weight=self.train[self.weight],
                             eval_set=[(self.test[self.X], y_test)],
                             eval_metric = 'auc',
                             callbacks=[lgb.early_stopping(20)]
        )
          

        # calculate performance metrices for final model
        self.rs = ks_search(self.train[self.X], self.train[self.y],
                            self.train[self.weight],
                            self.test[self.X], self.test[self.y],
                            self.test[self.weight])

        self.final_ksmetric = self.rs.randomsearch_KS(model=self.final_model)

        self.train_decile, self.test_decile, self.perf_metric = self.rs.result_ks(self.final_model)
        return True

    def hyperparameter_tuning(self, hyper_param_space):
        model_hptune = lgb.LGBMClassifier(n_estimators=int(hyper_param_space["n_estimators"]),
                                          max_depth=int(hyper_param_space["max_depth"]),
                                          reg_alpha=hyper_param_space["reg_alpha"],
                                          reg_lambda=hyper_param_space["reg_lambda"],
                                          learning_rate=hyper_param_space["learning_rate"],
                                          subsample=hyper_param_space["subsample"],
                                          pos_bagging_fraction=hyper_param_space["pos_bagging_fraction"],
                                          neg_bagging_fraction=hyper_param_space["neg_bagging_fraction"],
                                          colsample_bytree=hyper_param_space["colsample_bytree"],
                                          min_child_samples=int(hyper_param_space["min_child_samples"]),

                                          random_state=self.seed,
                                          bagging_fraction_seed=self.seed,
                                          feature_fraction_seed=self.seed,
                                          # cat_feature = f'name:{self.catg_features}',
                                          bagging_freq=5,
                                          objective='binary',
                                          n_jobs=self.n_jobs  # 2
                                          )

        # if kfold is not zero
        if self.kfolds != 0:
            # cross validation score
            skf = StratifiedKFold(n_splits=self.kfolds, random_state=self.seed, shuffle=True)
            score_list = cross_val_score(model_hptune, self.train[self.X], self.train[self.y], cv=skf)
            score = score_list.mean()

        # print("----------------------Training---------------------------")
        model_hptune.fit(
        X=self.train[self.X],
        y=self.train[self.y].values.ravel(),
        sample_weight=self.train[self.weight],
        eval_set=[(self.test[self.X], self.test[self.y].values.ravel())],
        callbacks=[lgb.early_stopping(20)]
        )
        print(hyper_param_space)

        # calculate metric results to show roc_auc curve
        y_prob = model_hptune.predict_proba(self.test[self.X])[:, 1]
        false_positive_rate, true_positive_rate, thresholds = roc_curve(self.test[self.y], y_prob)
        auc_roc = auc(false_positive_rate, true_positive_rate)

        # when kfold cross validation is not run
        try:
            print(f"[LOGS] mean cross validation score: {score}")
        except:
            score = auc_roc

        # y_pred = model_hptune.predict(self.test[self.X])
        # roc_auc = metrics.roc_auc_score(self.test[self.y], y_pred)
        # accuracy = accuracy_score(y_pred, self.test[self.y])

        # calculate different performance metrics such as ROC_AUC, Gini, KS 10%, 20% etc..
        rs = ks_search(self.train[self.X], self.train[self.y],
                       self.train[self.weight],
                       self.test[self.X], self.test[self.y],
                       self.test[self.weight])

        df_output_perf = rs.randomsearch_KS(model=model_hptune)

        ## append output of each iteration to spark custom accumulator
        ## for multi-node operation using spark distributed computing 
        # global output_lgb_acc
        # output_lgb_acc += [df_output_perf]

        # for single-node operation
        self.output_lgb.append(df_output_perf)

        return {"loss": 1 - score, "status": STATUS_OK}  # "loss": 1 - auc_roc
    
class mode_xgboost_bn:
    def __init__(self, df, param_dict):
        self.data = df
        self.seed = 25
        self.best = None
        self.output_xgb = []
        # self.__dict__.update(_dict)
        #self.n_parallel_models = param_dict['n_parallel_models']
        self.train = param_dict['train']
        self.test = param_dict['test']
        self.X = param_dict['X']
        self.y = param_dict['y']
        self.target = param_dict['target']
        self.weight = param_dict['weight']
        self.catg_features = param_dict.get('catg_features', [])
        self.n_jobs = param_dict.get('n_jobs', -1)
        self.n_iter = param_dict.get('n_iter', 10)
        self.kfolds = param_dict.get('kfolds', 0)

    def fit_multi_iter(self, *args, **kwargs):
        # convert categorical features to string (as required by catboost)
        for ele in self.catg_features:
            self.train[ele] = self.train[ele].astype("category")
            self.test[ele] = self.test[ele].astype("category")
        if hasattr(self, "custom_hyper_param_space"):
            hyper_param_space = self.custom_hyper_param_space
        else:
            hyper_param_space = {
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
        
        
        trials = Trials()   # single-node
        # trials = SparkTrials(parallelism=self.n_parallel_models)  # multi-node

        best = fmin(
            fn=self.hyperparameter_tuning,
            space=hyper_param_space,
            algo=tpe.suggest,
            max_evals=self.n_iter,  # 100
            trials=trials,
            rstate=np.random.default_rng(self.seed)
        )

        print("\n[INFO] Best set of parameters for XGBoost are below:\n", best)
        self.best = best

#         print(
#             "\n[INFO] Select model based on the KS table , Can be accessed by calling 'output_xgb_acc.value[<nth_iteration>]'. Once the model is selected pass the iteration number to fit_the_model function\n")

        print(
            "\n[INFO] Select model based on the KS table , Can be accessed by calling 'model.xgbmodel.output_xgb[<nth_iteration>]'. Once the model is selected pass the iteration number to best_iter_comp() function\n")

    def fit(self, nth_iteration=None):
        if nth_iteration is None:
            self.best['n_estimators'] = int(self.best['n_estimators'])
            self.best['max_depth'] = int(self.best['max_depth'])
            print(
                "[INFO] Training final model on Best set of hyper-parameters chosen after Hyperparameter tuning -->\n",
                self.best)
            model_param = self.best

        elif isinstance(nth_iteration, int):
            if nth_iteration <= self.n_iter and nth_iteration > 0:
                # get the dictionary of parameters for final model
                model_param = {}
                for key in self.best.keys():
                    model_param[key] = eval(self.output_xgb[nth_iteration]['param_dict'][0].replace('nan', '1'))[key]
                print(
                    f"[INFO] Training final model on set of hyper-parameters present in iteration number {nth_iteration} -->\n",
                    model_param)
            else:
                raise Exception(
                    "Invalid iteration number. Iteration number must be less than 'n_iter' and greater than 0.")

        else:
            raise Exception("Invalid iteration number. Iteration number must be an integer value")

        # Train final model
        self.final_model = xgb.XGBClassifier(**model_param,
                                             tree_method="hist",
                                             enable_categorical=True,
                                             # use_label_encoder=False,
                                             seed=self.seed,
                                             objective='binary:logistic',
                                             eval_metric='error',
                                             # error --> Binary classification error rate. calculated as #(wrong cases)/#(all cases)
                                             n_jobs=self.n_jobs  # 2
                                             )
        y_train = self.train[self.y].values.ravel()
        y_test  = self.test[self.y].values.ravel()

        self.final_model.fit(X=self.train[self.X], y=y_train,
                             sample_weight=self.train[self.weight],
                             eval_set=[(self.test[self.X], y_test)],  # , self.test[self.weight]
                             #eval_metric = "auc",
                             verbose=False

                             )

        # calculate performance metrices for final model
        self.rs = ks_search(self.train[self.X], self.train[self.y],
                            self.train[self.weight],
                            self.test[self.X], self.test[self.y],
                            self.test[self.weight])

        self.final_ksmetric = self.rs.randomsearch_KS(model=self.final_model)

        self.train_decile, self.test_decile, self.perf_metric = self.rs.result_ks(self.final_model)
        return True

    def hyperparameter_tuning(self, hyper_param_space):
        model_hptune = xgb.XGBClassifier(n_estimators=int(hyper_param_space['n_estimators']),
                                         max_depth=int(hyper_param_space['max_depth']),
                                         reg_alpha=hyper_param_space['reg_alpha'],
                                         reg_lambda=hyper_param_space['reg_lambda'],
                                         gamma=hyper_param_space['gamma'],
                                         learning_rate=hyper_param_space['learning_rate'],
                                         scale_pos_weight=hyper_param_space['scale_pos_weight'],
                                         colsample_bylevel=hyper_param_space['colsample_bylevel'],
                                         subsample=hyper_param_space['subsample'],

                                         tree_method="hist",
                                         enable_categorical=True,
                                         # use_label_encoder=False,
                                         seed=self.seed,
                                         objective='binary:logistic',
                                         eval_metric='error',
                                         # error --> Binary classification error rate. calculated as #(wrong cases)/#(all cases)
                                         n_jobs=self.n_jobs  # 2
                                         )

        # if kfold is not zero
        if self.kfolds != 0:
            # cross validation score
            skf = StratifiedKFold(n_splits=self.kfolds, random_state=self.seed, shuffle=True)
            score_list = cross_val_score(model_hptune, self.train[self.X], self.train[self.y], cv=skf)
            score = score_list.mean()

        # print("----------------------Training---------------------------")
        model_hptune.fit(X=self.train[self.X], y=self.train[self.y].values.ravel(),
                         sample_weight=self.train[self.weight],
                         eval_set=[(self.test[self.X], self.test[self.y].values.ravel())],  # , self.test[self.weight]
                        verbose=False

                         )
        print(hyper_param_space)

        # calculate metric results to show roc_auc curve
        y_prob = model_hptune.predict_proba(self.test[self.X])[:, 1]
        false_positive_rate, true_positive_rate, thresholds = roc_curve(self.test[self.y], y_prob)
        auc_roc = auc(false_positive_rate, true_positive_rate)

        # when kfold cross validation is not run
        try:
            print(f"[LOGS] mean cross validation score: {score}")
        except:
            score = auc_roc

        # y_pred = model_hptune.predict(self.test[self.X])
        # roc_auc = metrics.roc_auc_score(self.test[self.y], y_pred)
        # accuracy = accuracy_score(y_pred, self.test[self.y])

        # calculate different performace metrices such as ROC_AUC, Gini, KS 10%, 20% etc..
        rs = ks_search(self.train[self.X], self.train[self.y],
                       self.train[self.weight],
                       self.test[self.X], self.test[self.y],
                       self.test[self.weight])

        df_output_perf = rs.randomsearch_KS(model=model_hptune)

        ## append output of each iteration to spark custom accumulator
        ## for multi-node operation using spark distributed computing 
        # global output_xgb_acc
        # output_xgb_acc += [df_output_perf]

        # for single-node operation
        self.output_xgb.append(df_output_perf)

        return {"loss": 1 - score, "status": STATUS_OK}  # "loss": 1 - auc_roc


