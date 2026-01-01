# Databricks notebook source
# Import libraries
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from operator import add
from scipy.stats import ttest_ind, chi2_contingency
from library import nl_utils
from sql_scripts import sql_query_OOT

from pathlib import Path
from datetime import timedelta, date
from handlers import Session
from dateutil.relativedelta import relativedelta
from snowflake.connector.pandas_tools import write_pandas


if __name__ == "__main__":

    version = 'V1'
    model_type = 'Prescreen_Resp'


    session = Session(use_log=True)
    session.log.info("Running 1.Data_pull.py...")
    mail_month = session.mail_dt.strftime('%Y%m')
    mail_month_sql = session.mail_dt.strftime('%Y%m%d')
    run_month_sql =   session.run_dt.strftime('%Y%m%d')
    excluded_cols = [ ]
    final_selected_cols = []


    modeling_tbl_name = f'sandbox_datascience.mkelley.{model_type}_MODEL_{version}'
    base_df = spark.table(modeling_tbl_name).toPandas().sort_values(by=['experianKey'])
    base_df = base_df[base_df.columns[base_df.columns.isin(final_selected_cols)]]
    base_df.shape

    # Correlation matrix
    corr = base_df.corr()
    corr = corr.round(2)
    plt.figure(figsize=(20, 16))
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(
        corr,
        mask=mask,
        cmap='coolwarm',
        center=0,
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.5}
    )

    plt.title("Feature Correlation Heatmap", fontsize=18)
    plt.tight_layout()
    plt.show()


