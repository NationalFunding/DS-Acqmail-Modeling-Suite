
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from operator import add
from scipy.stats import ttest_ind, chi2_contingency
import pandas as pd
from library import nl_utils
from sql_scripts import sql_query_OOT
import numpy as np
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


# --------- Will we have any scores to compare with? ---------------

# model version in nf_workarea.historical_campaign_scores for reference scores
# reference_model_score_version_resp = 'V6 RESP'
# reference_model_score_version_sub = 'V6 SUB'

experian_data = pd.read_csv("NAME OF MODELING CSV FROM EXPERIAN (IN S3 BUCKET)")
table_output_name = f'{model_type}_MODEL_{version}'

success, nchunks, nrows, _ = write_pandas(
    conn=session.snowflake._conn,  
    df=experian_data,
    table_name=table_output_name,
    schema="MKELLEY",
    database="SANDBOX_DATASCIENCE",
    auto_create_table=True
)
