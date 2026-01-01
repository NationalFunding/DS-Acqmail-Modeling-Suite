# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import json
import gc
import numpy as np


mdl_stg_str = '["RESP", "SUB"]'
OUTPUT_FOLDER = '/dbfs/FileStore/shared_uploads/model_development/responseplusv7_prescreen'
ScoreDataInputName = 'no existing data'
IfSaveScoreDataInput = True

IfSaveScoreDataInput = True if ScoreDataInputName == "no existing data" else False
version = 'V7'

# COMMAND ----------

# MAGIC %run ./_session
# MAGIC

# COMMAND ----------

session = NatFunSession()

# COMMAND ----------

# MAGIC %md
# MAGIC # Collect Baseline Data

# COMMAND ----------

#output = f'{session.db}.score_data_input'

# COMMAND ----------

#MK edited
###############################################
# Generate model data input shared with both brands
################################################
mth_title = session.mth_title
print(f'Generating table for mail date {session.mail_date_str} ({session.mth_title}) ')
if ScoreDataInputName != "no existing data":
    import_data_sql = f"""create or replace table nf_dev_workarea.{version}_score_data_input_{mth_title} as
                            select * from {ScoreDataInputName}"""
    spark.sql(import_data_sql)
    session.log.info(f"Skip data generation and imported {ScoreDataInputName}")
    output = f'{version}_score_data_input_{mth_title}'

else:
    query = f"""
        with nf as (
        select duns
        , True as nf_mailable
        , lastmailmonth as nf_last_mail_date
        , firstmailmonth as nf_first_mail_date
        , totaltimesmailed as nf_num_times_mailed
        , cadencepattern as nf_cadence_pattern
        , custom_response as nf_custom_response
        , custom_funded as nf_custom_funded
        from nf_workarea.acquisitionmailhistory_nf
        where mailrunmonth = '{session.mail_date_str}'
        ), qb as (
        select duns
        , True as qb_mailable
        , lastmailmonth as qb_last_mail_date
        , firstmailmonth as qb_first_mail_date
        , totaltimesmailed as qb_num_times_mailed
        , cadencepattern as qb_cadence_pattern
        , custom_response as qb_custom_response
        , custom_funded as qb_custom_funded
        from nf_workarea.acquisitionmailhistory_qb
        where mailrunmonth = '{session.mail_date_str}'
        )
    select coalesce(nf.duns, qb.duns) as duns
    , year(add_months('{session.mail_date_str}', -3)) as load_year 
    , month(add_months('{session.mail_date_str}', -3)) as load_month
    , year(add_months('{session.mail_date_str}', -4)) as load_year_prior
    , month(add_months('{session.mail_date_str}', -4)) as load_month_prior
    , coalesce(nf.nf_custom_response, qb.qb_custom_response) as custom_response
    , coalesce(nf.nf_custom_funded, qb.qb_custom_funded) as custom_funded
    , nf_mailable, nf_last_mail_date, nf_first_mail_date, nf_num_times_mailed, nf_cadence_pattern
    , qb_mailable, qb_last_mail_date, qb_first_mail_date, qb_num_times_mailed, qb_cadence_pattern
    from nf full join qb on nf.duns=qb.duns
    """
    historical_campaign_score_pop_spark = spark.sql(query)
    historical_campaign_score_pop_spark.write.mode('overwrite').format('parquet').saveAsTable(f'{session.db}.{version}_{mth_title}_historical_campaign_score_pop')
    session.log.info(f"Done saving {session.db}.{version}_{mth_title}_historical_campaign_score_pop")

    # write to DB
    output = f'{session.db}.{version}_score_data_input_{mth_title}'
    drop_sql = """ drop table if exists """ + output
    spark.sql(drop_sql)

    model_data_input = session.query_model_data(model_version=version)
    model_data_input.write.mode("overwrite").format("parquet").saveAsTable(output)
    session.log.info(f"Done saving {output}")

if IfSaveScoreDataInput:         
    SaveScoreDataInputName = f"nf_dev_workarea.{version}_score_data_input_{session.mail_date_str.replace('-','_')}"       
    replace_table_sql = f""" create or replace table as {SaveScoreDataInputName}
                            select * from {output} """
    session.log.info(f"Done saving {SaveScoreDataInputName}")

# COMMAND ----------

query = spark.sql(f"""SELECT COUNT(*) FROM {session.db}.{version}_score_data_input_{mth_title}""") 
result = query.collect()
print(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from nf_dev_workarea.v7_score_data_input_jul2025
# MAGIC where rdi_indicator is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nf_dev_workarea.v7_score_data_input_jul2025

# COMMAND ----------

# %sql
# select RUN_DATE, count(*)
# from nationalfunding_sf_share.load_as_mail_history_qb
# group by 1

# COMMAND ----------

#MK edited

attr_for_selection = ['nf_mailable', 'qb_mailable', 'TIB', 'gc_employees', 'gc_sales', 'ccs_points', 'chg_flg', 'nf_num_times_mailed', 'qb_num_times_mailed', 'ba_cnt_cr_12m', 'inq_inquiry_duns_24m', 'ppp']


################################################
# Generate model data input shared with both brands
################################################
# model_data_input = session.query_model_data(model_version = version)
# model_data_input.write.mode("overwrite").format("parquet").saveAsTable(f'{session.db}.model_data_input_{version}')

#session.check_model_data()

# del model_data_input
# gc.collect()

nfqb = {}
for class_ in ['c1', 'c2']:
    brand_dfs = {}

    for brand in ['nf', 'qb']:
        final_scores_brand = pd.DataFrame()
        
        for flg_ppp_ in ['N','Y']:

            for change_ in [0, 1]:
                
                session.log.info(f"################################################################")
                model_input = spark.sql(f"""SELECT * FROM {session.db}.{version}_score_data_input_{mth_title}
                                            WHERE class_type = '{class_}' AND ppp = '{flg_ppp_}' AND chg_flg = {change_} 
                                            """) 

                pandas_input = session.conduct_brand_specific_feature_engineering(model_data=model_input)
                session.log.info(f"Model dataframe has {len(pandas_input): 6,} available records")
                
                scored_duns = pandas_input[['duns'] + attr_for_selection] #for combining two model types: RESP & SUB

                for model_ in ['RESP', 'SUB']:

                    session.batch_scoring.identify_model_list(modeling_data_source=pandas_input
                                                            , source_tab_brand=brand
                                                            , class_type=class_
                                                            , model_type=model_
                                                            , ppp=flg_ppp_
                                                            , version = version)

                    dffs = session.batch_scoring.score_batch(version = version)

                    dffs.rename(columns={'proba': f'{brand}_proba_{model_}'
                                        , 'mdl_name': f'{brand}_mdl_name_{model_}'}, inplace=True)
                    
                    scored_duns = scored_duns.merge(dffs, how='left', on='duns')

                session.log.info(f"Done scoring the segment with {len(scored_duns): 6,} available records")
                final_scores_brand = pd.concat([final_scores_brand, scored_duns], axis=0)

        brand_dfs[brand] = final_scores_brand.copy()


        session.log.info(f"Done scoring brand {brand} with {len(final_scores_brand): 6,} available records")
        
    nfqb_scores = brand_dfs['nf'].merge(
        brand_dfs['qb'],
        on='duns',
        how='outer'
    )

    nfqb[class_] = nfqb_scores
    session.log.info(f"Done class {class_} scoring")

# COMMAND ----------


with open(STAGING_FOLDER + f'/nfqb_{version}_{mth_title}.pkl', 'wb') as handle:
    pickle.dump(nfqb, handle, protocol=pickle.HIGHEST_PROTOCOL)

########################
# CLEAN UP
########################

del final_scores_brand, scored_duns, pandas_input, dffs, nfqb_scores, nfqb, model_input
gc.collect()

# COMMAND ----------

file_path = STAGING_FOLDER + f'/nfqb_{version}_{mth_title}.pkl'

with open(file_path, 'rb') as handle:
    nfqb_loaded = pickle.load(handle)

# COMMAND ----------


df_c1 = nfqb_loaded["c1"]
df_c2 = nfqb_loaded["c2"]

df_all = pd.concat([df_c1, df_c2], ignore_index=True)



# COMMAND ----------

df_all.shape

# COMMAND ----------

df_all.head()

# COMMAND ----------

df_filtered = df_all[df_all["nf_proba_RESP"] != df_all["qb_proba_RESP"]]
df_filtered.shape

# COMMAND ----------

score_bins = {
    'V7': {
        'RESP': [0, 0.034114, 0.068916, 0.111961, 0.162962, 0.222099, 
                 0.284987, 0.354618, 0.437950, 0.543222, 1],
        
        'SUB' : [0, 0.186932, 0.266917, 0.325710, 0.377214, 0.426287, 
                 0.479018, 0.536608, 0.607101, 0.705895, 1]
    }
}

# COMMAND ----------

df_all["nf_resp_score_bin"] = pd.cut(
    df_all["nf_proba_RESP"],
    bins=score_bins["V7"]["RESP"],
    include_lowest=True,
    duplicates="drop"
)


# COMMAND ----------

df_all["nf_resp_score_bin"] = df_all["nf_resp_score_bin"].cat.as_ordered()

# COMMAND ----------


bin_counts = df_all["nf_resp_score_bin"].value_counts(sort=False)


summary = pd.DataFrame({
    "score_bin": bin_counts.index.astype(str),
    "count": bin_counts.values
})


summary["pct_total"] = summary["count"] / summary["count"].sum()
summary["decile"] = list(range(len(summary), 0, -1))
summary = summary.iloc[::-1].reset_index(drop=True)
summary["decile"] = summary.index + 1
summary

# COMMAND ----------

df_all["nf_sub_score_bin"] = pd.cut(
    df_all["nf_proba_SUB"],
    bins=score_bins["V7"]["SUB"],
    include_lowest=True,
    duplicates="drop"
)

# COMMAND ----------

# Count rows per bin
bin_counts = df_all["nf_sub_score_bin"].value_counts(sort=False)

# Make into DataFrame
summary = pd.DataFrame({
    "score_bin": bin_counts.index.astype(str),
    "count": bin_counts.values
})

# Percent of total
summary["pct_total"] = summary["count"] / summary["count"].sum()
summary["decile"] = list(range(len(summary), 0, -1))
summary = summary.iloc[::-1].reset_index(drop=True)
summary["decile"] = summary.index + 1
summary

# COMMAND ----------

df_all