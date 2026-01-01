# Databricks notebook source
import datetime
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import seaborn as sns
from pandas_profiling import ProfileReport 
from scipy.stats import chi2_contingency, ttest_ind
from pyspark.sql.types import *
from functools import reduce
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from operator import add
from library import nl_utils
from pathlib import Path
from handlers import Session
from dateutil.relativedelta import relativedelta
from snowflake.connector.pandas_tools import write_pandas


if __name__ == "__main__":

    version = 'V1'
    model_type = 'Prescreen_Resp'
    label = 'flg_resp'




    prescreen_resp_modeling = session.snowflake.read_script(
                        sql_script_path=f'sql_scripts/1.get_modeling_table.sql')


    # modeling_tbl_name = f'sandbox_datascience.mkelley.{model_type}_MODEL_{version}'
    # base_df = spark.table(modeling_tbl_name).toPandas().sort_values(by=['experianKey'])
    # base_df = base_df[base_df.columns[base_df.columns.isin(final_selected_cols)]]
    # base_df.shape


    excluded_cols = ['experian_key', label]
    cat_cols = [item[0] for item in df.dtypes if ((item[1].startswith('string')) | (item[0].startswith('chg'))) & (item[0] not in excluded_cols)] + [label]
    num_cols = [item[0] for item in df.dtypes if (item[0] not in cat_cols) & (item[0] not in excluded_cols)] + [label]

    num_df = df.select(*num_cols).toPandas()
    cat_df = df.select(*cat_cols).toPandas()


    num_df[label].value_counts(normalize = True)

 

    num_df[label].value_counts()


    displayHTML(ProfileReport(num_df, minimal=True).html)

    displayHTML(ProfileReport(cat_df, minimal=True).html)


    def categorical_chi2_test(df, col, label):

        contigency= pd.crosstab(df[col], df[label], dropna = False)
        c, p, dof, expected = chi2_contingency(contigency)

        return p

    def numerical_t_test(df, col, label):

        group_0 = df[df[label] == 0]
        group_1 = df[df[label] == 1]
        t_stat = ttest_ind(group_0[col], group_1[col], nan_policy = 'omit')

        return t_stat.pvalue

    print("Significant Categorical Variables")
    sig_cat_cols = []
    for cols in cat_cols:
        try:
            p_value = categorical_chi2_test(cat_df, cols, label)
            if p_value < 0.01:
                sig_cat_cols.append(cols)
        except:
            pass
    print(sig_cat_cols)

    print("Significant Numerical Variables")
    sig_num_cols = []
    for cols in num_cols:
        try:
            p_value = numerical_t_test(num_df, cols, label)
        except:
            continue
        if p_value < 0.01:
            sig_num_cols.append(cols)
    print(sig_num_cols)

  

    len(list(set(sig_num_cols + sig_cat_cols)))



    len(list(set(sig_num_cols + sig_cat_cols)))/len(list(set(num_cols + cat_cols)))



    len(list(set(sig_num_cols + sig_cat_cols)))/len(list(set(num_cols + cat_cols)))



    pd.crosstab(cat_df['distribution_tier_bivariate'], cat_df['rents_ind'], dropna = False, normalize='columns')



    pd.crosstab(cat_df['distribution_tier_bivariate'], cat_df['rdi_indicator'], dropna = False, normalize='columns')



    pd.crosstab(cat_df['distribution_tier_bivariate'], cat_df['ppp'], dropna = False, normalize='columns')

 

    num_df.isnull().sum().sort_values(ascending=False).head(50)/num_df.shape[0]