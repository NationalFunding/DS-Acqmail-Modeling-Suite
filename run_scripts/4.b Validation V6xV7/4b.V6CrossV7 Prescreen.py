# Databricks notebook source
### Pulling Prescreen data from Snowflake QUERY!!!
''' 
with prescreen AS (
select distinct campaign_id, source__c
from ods.salesforce_nf.sf_nf_campaign
where year(enddate)=2025 and month(enddate) in (6,7,8)
and prospect_type__c='Acquisition'
and list_reference__c like 'Prescreen'

)


select duns, run_date, brand_code, account_num from dnb.acquisitionmail.acq_mail_campaign_append a
left join prescreen p ON p.source__c = left(a.account_num,4)
where p.campaign_id is NOT NULL
and a.run_date in ('20250401','20250501','20250601')






'''

# COMMAND ----------

# File location and type
file_location = "dbfs:/FileStore/tables/prescreen_jun_aug.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

temp_table_name = "prescreen_jun_aug_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH dat as
# MAGIC (
# MAGIC     SELECT duns,
# MAGIC     run_date
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), -1) AS load_date
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), 2) AS mail_date
# MAGIC     from prescreen_jun_aug_csv
# MAGIC     )
# MAGIC select count(*) from dat

# COMMAND ----------

# %sql
# -- v6_cross_v7 = spark.sql("""
# with final_scored_table AS (
# WITH dat as (
#     WITH dat_stage as
# (
#     SELECT duns,
#     to_date(run_date, 'yyyyMMdd') AS run_date_ps
#     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), -1) AS load_date_ps
#     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), 2) AS mail_date_ps
#     from prescreen_jun_aug_csv
#     ),
# campaign_hist AS (
#     SELECT
#         COALESCE(nf.duns, qb.duns) AS duns,
#         nf.campaignid AS nf_campaignid,
#         qb.campaignid AS qb_campaignid,
#         nf.accountnum AS nf_accountnum,
#         qb.accountnum AS qb_accountnum,
#         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date,
#         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date,
#         COALESCE(nf.mail_date, qb.mail_date) AS mail_date,
#         CASE WHEN nf.duns IS NOT NULL THEN TRUE END AS nf_mailable,
#         CASE WHEN qb.duns IS NOT NULL THEN TRUE END AS qb_mailable
#     FROM (
#         SELECT *
#         FROM nf_workarea.AcquisitionMailHistory_NF
#         WHERE mail_date BETWEEN '2025-02-01' AND '2025-08-01'
#           AND list != 'Prescreen Remail/V6'
#     ) nf
#     FULL OUTER JOIN (
#         SELECT *
#         FROM nf_workarea.AcquisitionMailHistory_QB
#         WHERE mail_date BETWEEN '2025-02-01' AND '2025-08-01'
#           AND (list != 'Prescreen Remail/V6' OR list IS NULL)
#     ) qb
#         ON nf.duns = qb.duns
#        AND nf.run_date = qb.run_date
# ),
# campaign_match AS (
#     SELECT
#         dat_stage.*,
#         ch.nf_campaignid,
#         ch.qb_campaignid,
#         ch.nf_accountnum,
#         ch.qb_accountnum,
#         ch.nf_mailable,
#         ch.qb_mailable,
#         ch.load_date,
#         ch.run_date,
#         ch.mail_date,
#         ROW_NUMBER() OVER (
#             PARTITION BY dat_stage.duns, dat_stage.run_date_ps
#             ORDER BY ch.run_date DESC
#         ) AS rn
#     FROM dat_stage
#     LEFT JOIN campaign_hist ch
#         ON dat_stage.duns = ch.duns
#        AND ch.run_date <= dat_stage.run_date_ps
# ),
# campaign_final AS (
#     SELECT * FROM campaign_match WHERE rn = 1
# )
# select * from campaign_final
# ),
# staging AS (
# SELECT dat.*
#     ,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
#     , nf_resp.proba AS nf_proba_RESP_7
#     , nf_resp.mdl_name AS nf_mdl_name_RESP_7
#     , nf_sub.proba AS nf_proba_SUB_7
#     , nf_sub.mdl_name AS nf_mdl_name_SUB_7
#     , qb_resp.proba AS qb_proba_RESP_7
#     , qb_resp.mdl_name AS qb_mdl_name_RESP_7
#     , qb_sub.proba AS qb_proba_SUB_7
#     , qb_sub.mdl_name AS qb_mdl_name_SUB_7
#     , nf_resp_6.proba AS nf_proba_RESP_6
#     , nf_resp_6.mdl_name AS nf_mdl_name_RESP_6
#     , nf_sub_6.proba AS nf_proba_SUB_6
#     , nf_sub_6.mdl_name AS nf_mdl_name_SUB_6
#     , qb_resp_6.proba AS qb_proba_RESP_6
#     , qb_resp_6.mdl_name AS qb_mdl_name_RESP_6
#     , qb_sub_6.proba AS qb_proba_SUB_6
#     , qb_sub_6.mdl_name AS qb_mdl_name_SUB_6


#     , CASE WHEN ppp_df.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp

#      , COALESCE(nfp.flg_resp, qbp.flg_resp) AS flg_resp
#     , COALESCE(nfp.flg_qual, qbp.flg_qual) AS flg_qual
#     , COALESCE(nfp.flg_sub, qbp.flg_sub) AS flg_sub
#     , COALESCE(nfp.flg_appr, qbp.flg_appr) AS flg_appr
#     , COALESCE(CASE WHEN nfp.internal_approved_amount > 0 THEN 1 ELSE 0 END, CASE WHEN qbp.internal_approved_amount > 0 THEN 1 ELSE 0 END) AS flg_appr_obs
#     , COALESCE(nfp.flg_fund, qbp.flg_fund) AS flg_fund
#     , COALESCE(CASE WHEN nfp.internal_funded_amount > 0 THEN 1 ELSE 0 END, CASE WHEN qbp.internal_funded_amount > 0 THEN 1 ELSE 0 END) AS flg_fund_obs
#     , COALESCE(nfp.fund_amt, qbp.fund_amt) AS fund_amt
#     , COALESCE(nfp.internal_funded_amount, qbp.internal_funded_amount) AS fund_amt_obs
#     , COALESCE(CASE WHEN nfp.internal_approved_amount > 0 THEN 1 ELSE 0 END, CASE WHEN qbp.internal_approved_amount > 0 THEN 1 ELSE 0 END) AS flg_appr_obs
  , COALESCE(CASE WHEN nfp.internal_funded_amount > 0 THEN 1 ELSE 0 END, CASE WHEN qbp.internal_funded_amount > 0 THEN 1 ELSE 0 END) AS flg_fund_obs
#     , COALESCE(nfp.internal_funded_amount, qbp.internal_funded_amount) AS fund_amt_obs

# FROM dat 
#     INNER JOIN us_marketing.dnb_core_id_geo_us af
#     ON dat.duns = af.duns AND YEAR(dat.load_date) = af.load_year AND MONTH(dat.load_date) = af.load_month
#     INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
#     ON af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month

#     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
#     ON dat.duns = nf_resp.duns AND dat.run_date = nf_resp.run_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = 'V7 RESP'
#     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
#     ON dat.duns = nf_sub.duns AND dat.run_date = nf_sub.run_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = 'V7 SUB'
#     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
#     ON dat.duns = qb_resp.duns AND dat.run_date = qb_resp.run_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = 'V7 RESP'
#     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
#     ON dat.duns = qb_sub.duns AND dat.run_date = qb_sub.run_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = 'V7 SUB'

#     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp_6
#     ON dat.duns = nf_resp_6.duns AND dat.run_date = nf_resp_6.run_date AND nf_resp_6.brand = 'NF' AND nf_resp_6.model_version = 'V6 RESP'
#     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub_6
#     ON dat.duns = nf_sub_6.duns AND dat.run_date = nf_sub_6.run_date AND nf_sub_6.brand = 'NF' AND nf_sub_6.model_version = 'V6 SUB'
#     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp_6
#     ON dat.duns = qb_resp_6.duns AND dat.run_date = qb_resp_6.run_date AND qb_resp_6.brand = 'QB' AND qb_resp_6.model_version = 'V6 RESP'
#     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub_6
#     ON dat.duns = qb_sub_6.duns AND dat.run_date = qb_sub_6.run_date AND qb_sub_6.brand = 'QB' AND qb_sub_6.model_version = 'V6 SUB'

#     LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp_df
#     ON ppp_df.EXTERNAL_ID_MARKETING=dat.duns

#     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_nf_2025111 nfp
#     ON dat.nf_accountnum=nfp.accountnum  AND nfp.RUN_DATE = '20251101'
#     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_qb_2025111 qbp
#     ON dat.qb_accountnum=qbp.accountnum AND qbp.RUN_DATE = '20251101'
#    -- WHERE dat.mdl_name_RESP IS NOT NULL
# ),

# staging_2 AS (
#   select *
#       , COALESCE(nf_proba_RESP_7, qb_proba_RESP_7) AS resp_v7
#     ,COALESCE(nf_proba_RESP_6, qb_proba_RESP_6) AS resp_v6
#     , COALESCE(nf_proba_SUB_7, qb_proba_SUB_7) AS sub_v7
#     ,COALESCE(nf_proba_SUB_6, qb_proba_SUB_6) AS sub_v6
#     from staging
# )
# SELECT *

#     , CASE
#     WHEN resp_v7 < 0.034114 THEN 1
#     WHEN resp_v7 < 0.068916 THEN 2
#     WHEN resp_v7 < 0.111961 THEN 3
#     WHEN resp_v7 < 0.162962 THEN 4
#     WHEN resp_v7 < 0.222099 THEN 5
#     WHEN resp_v7 < 0.284987 THEN 6
#     WHEN resp_v7 < 0.354618 THEN 7
#     WHEN resp_v7 < 0.437950 THEN 8
#     WHEN resp_v7 < 0.543222 THEN 9
#     ELSE 10
#   END AS v7_resp_bin
#   , CASE 
#     WHEN ppp = 'N' THEN CASE
#         WHEN resp_v6 < 0.06986177294190464 THEN 1
#         WHEN resp_v6 < 0.1339245864382257 THEN 2
#         WHEN resp_v6 < 0.19911057548148098 THEN 3
#         WHEN resp_v6 < 0.2648634399966438 THEN 4
#         WHEN resp_v6 < 0.3308196007628543 THEN 5
#         WHEN resp_v6 < 0.4020418214049535 THEN 6
#         WHEN resp_v6 < 0.4799049832521699 THEN 7
#         WHEN resp_v6 < 0.5674352790225993 THEN 8
#         WHEN resp_v6 < 0.6865566088244687 THEN 9
#         ELSE 10
#     END
#         WHEN ppp = 'Y' THEN CASE
#         WHEN resp_v6 < 0.09192317674472977 THEN 1
#         WHEN resp_v6 < 0.14241300658817654 THEN 2
#         WHEN resp_v6 < 0.19302681254587684 THEN 3
#         WHEN resp_v6 < 0.24772050172405718 THEN 4
#         WHEN resp_v6 < 0.3096403794311008 THEN 5
#         WHEN resp_v6 < 0.38130716726325864 THEN 6
#         WHEN resp_v6 < 0.46656576625518276 THEN 7
#         WHEN resp_v6 < 0.5702527435980795 THEN 8
#         WHEN resp_v6 < 0.7003898538311522 THEN 9
#         ELSE 10
#     END
#   END AS v6_resp_bin,
#   CASE
#     WHEN sub_v7 < 0.186932 THEN 1
#     WHEN sub_v7 < 0.266917 THEN 2
#     WHEN sub_v7 < 0.325710 THEN 3
#     WHEN sub_v7 < 0.377214 THEN 4
#     WHEN sub_v7 < 0.426287 THEN 5
#     WHEN sub_v7 < 0.479018 THEN 6
#     WHEN sub_v7 < 0.536608 THEN 7
#     WHEN sub_v7 < 0.607101 THEN 8
#     WHEN sub_v7 < 0.705895 THEN 9
#     ELSE 10
# END AS v7_sub_bin,
# CASE 
#     WHEN ppp = 'N' THEN CASE
#         WHEN sub_v6 < 0.4003984111765917 THEN 1
#         WHEN sub_v6 < 0.4830520207143802 THEN 2
#         WHEN sub_v6 < 0.5389749091311864 THEN 3
#         WHEN sub_v6 < 0.5860559515801678 THEN 4
#         WHEN sub_v6 < 0.6304661992424985 THEN 5
#         WHEN sub_v6 < 0.6753514420846224 THEN 6
#         WHEN sub_v6 < 0.7227107976823965 THEN 7
#         WHEN sub_v6 < 0.7758924349663294 THEN 8
#         WHEN sub_v6 < 0.8256361927932095 THEN 9
#         ELSE 10
#     END
#         WHEN ppp = 'Y' THEN CASE
#         WHEN sub_v6 < 0.29706895621726714 THEN 1
#         WHEN sub_v6 < 0.3885554922757615 THEN 2
#         WHEN sub_v6 < 0.462956220812887 THEN 3
#         WHEN sub_v6 < 0.5217694835605138 THEN 4
#         WHEN sub_v6 < 0.5703851279083824 THEN 5
#         WHEN sub_v6 < 0.6102073280311974 THEN 6
#         WHEN sub_v6 < 0.6428669435988154 THEN 7
#         WHEN sub_v6 < 0.6702748121587252 THEN 8
#         WHEN sub_v6 < 0.6960303488028483 THEN 9
#         ELSE 10
#     END
# END AS v6_sub_bin

#     FROM Staging_2)

# SELECT 
#     v6_resp_bin,
#     v7_resp_bin,
#     COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS account_count,
#     SUM(flg_resp),
#     sum(flg_resp)/ COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS resp_rate,
#     SUM(flg_sub),
#     SUM(flg_sub)/sum(flg_resp) as sub_rate,
#     SUM(flg_appr),
#     SUM(flg_appr)/sum(flg_sub) AS appr_rate,
#     sum(flg_appr_obs),
#     SUM(flg_appr_obs)/sum(flg_sub) AS appr_obs_rate,
#     sum(flg_fund),
#     sum(flg_fund)/sum(flg_appr) as appr_to_fund_rate,
#     sum(flg_fund)/sum(flg_appr_obs) as obs_appr_to_fund_rate,
#     sum(flg_fund)/sum(flg_sub) as sub_to_fund_rate,
#     sum(flg_fund)/sum(flg_resp) as resp_to_fund_rate,
#     sum(flg_fund_obs),
#     sum(flg_fund_obs)/sum(flg_appr) as appr_to_fund_obs_rate,
#     sum(flg_fund_obs)/sum(flg_appr_obs) as obs_appr_to_obs_fund_rate,
#     sum(flg_fund_obs)/sum(flg_sub) as sub_to_fund_obs_rate,
#     sum(flg_fund_obs)/sum(flg_resp) as resp_to_fund_obs_rate,
#     sum(fund_amt),
#     sum(fund_amt_obs)
# FROM final_scored_table 
# GROUP BY 1,2
# ORDER BY 1,2;


# COMMAND ----------

# MAGIC %sql
# MAGIC  -- PRESCREEN SUB
# MAGIC
# MAGIC     WITH dat_stage as
# MAGIC (
# MAGIC     SELECT duns,
# MAGIC     to_date(run_date, 'yyyyMMdd') AS run_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), -1) AS load_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), 2) AS mail_date_ps,
# MAGIC     CASE WHEN run_date = '20250401' THEN to_date('2025-03-01') 
# MAGIC     WHEN run_date = '20250501' THEN to_date('2025-05-01')
# MAGIC     WHEN run_date = '20250601' THEN to_date('2025-06-01') END AS bau_campaign_match_mail_date
# MAGIC     from prescreen_jun_aug_csv
# MAGIC     ),
# MAGIC campaign_hist AS (
# MAGIC     SELECT
# MAGIC         COALESCE(nf.duns, qb.duns) AS duns,
# MAGIC         nf.campaignid AS nf_campaignid,
# MAGIC         qb.campaignid AS qb_campaignid,
# MAGIC         nf.accountnum AS nf_accountnum,
# MAGIC         qb.accountnum AS qb_accountnum,
# MAGIC         nf.list as nf_list,
# MAGIC         qb.list as qb_list,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date,
# MAGIC         COALESCE(nf.mail_date, qb.mail_date) AS mail_date,
# MAGIC         CASE WHEN nf.duns IS NOT NULL THEN TRUE END AS nf_mailable,
# MAGIC         CASE WHEN qb.duns IS NOT NULL THEN TRUE END AS qb_mailable
# MAGIC     FROM (
# MAGIC         SELECT *
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_NF
# MAGIC         WHERE mail_date BETWEEN '2025-03-01' AND '2025-08-01'
# MAGIC           --AND list != 'Prescreen Remail/V6'
# MAGIC     ) nf
# MAGIC     FULL OUTER JOIN (
# MAGIC         SELECT *
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_QB
# MAGIC         WHERE mail_date BETWEEN '2025-03-01' AND '2025-08-01'
# MAGIC           --AND (list != 'Prescreen Remail/V6' OR list IS NULL)
# MAGIC     ) qb
# MAGIC         ON nf.duns = qb.duns
# MAGIC        AND nf.mail_date = qb.mail_date
# MAGIC ),
# MAGIC campaign_match AS (
# MAGIC     SELECT
# MAGIC         dat_stage.*,
# MAGIC         ch.duns AS campaign_hist_duns,
# MAGIC         ch.nf_list,
# MAGIC         ch.qb_list,
# MAGIC         ch.nf_campaignid,
# MAGIC         ch.qb_campaignid,
# MAGIC         ch.nf_accountnum,
# MAGIC         ch.qb_accountnum,
# MAGIC         ch.nf_mailable,
# MAGIC         ch.qb_mailable,
# MAGIC         ch.load_date,
# MAGIC         ch.run_date,
# MAGIC         ch.mail_date
# MAGIC     FROM dat_stage
# MAGIC     LEFT JOIN campaign_hist ch
# MAGIC     ON dat_stage.duns = ch.duns
# MAGIC     AND dat_stage.bau_campaign_match_mail_date = ch.mail_date
# MAGIC ),
# MAGIC campaign_final AS (
# MAGIC     SELECT * FROM campaign_match
# MAGIC )
# MAGIC select mail_date_ps, sum(campaign_hist_duns_count), sum(duns_count),
# MAGIC sum(campaign_hist_duns_count) / sum(duns_count) as ratio from (select mail_date_ps, count(distinct campaign_hist_duns) as campaign_hist_duns_count, count(distinct duns) as duns_count from campaign_final group by mail_date_ps) t group by mail_date_ps
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRESCREEN RESP
# MAGIC  with final_scored_table AS (
# MAGIC  WITH dat as (
# MAGIC     WITH dat_stage as
# MAGIC (
# MAGIC     SELECT duns,
# MAGIC     to_date(run_date, 'yyyyMMdd') AS run_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), -1) AS load_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), 2) AS mail_date_ps,
# MAGIC     CASE WHEN run_date = '20250401' THEN to_date('2025-03-01') 
# MAGIC     WHEN run_date = '20250501' THEN to_date('2025-05-01')
# MAGIC     WHEN run_date = '20250601' THEN to_date('2025-06-01') END AS bau_campaign_match_mail_date
# MAGIC     from prescreen_jun_aug_csv
# MAGIC     ),
# MAGIC campaign_hist AS (
# MAGIC     SELECT
# MAGIC         COALESCE(nf.duns, qb.duns) AS duns,
# MAGIC         nf.campaignid AS nf_campaignid,
# MAGIC         qb.campaignid AS qb_campaignid,
# MAGIC         nf.accountnum AS nf_accountnum,
# MAGIC         qb.accountnum AS qb_accountnum,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date,
# MAGIC         COALESCE(nf.mail_date, qb.mail_date) AS mail_date,
# MAGIC         CASE WHEN nf.duns IS NOT NULL THEN TRUE END AS nf_mailable,
# MAGIC         CASE WHEN qb.duns IS NOT NULL THEN TRUE END AS qb_mailable
# MAGIC     FROM (
# MAGIC         SELECT distinct campaignid, accountnum, mail_date, duns
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_NF
# MAGIC         WHERE mail_date in ('2025-03-01','2025-05-01', '2025-06-01')
# MAGIC           AND list != 'Prescreen Remail/V6'
# MAGIC     ) nf
# MAGIC     FULL OUTER JOIN (
# MAGIC         SELECT  distinct campaignid, accountnum, mail_date, duns
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_QB
# MAGIC         WHERE   mail_date in ('2025-03-01','2025-05-01', '2025-06-01')
# MAGIC           AND (list != 'Prescreen Remail/V6' OR list IS NULL)
# MAGIC     ) qb
# MAGIC         ON nf.duns = qb.duns
# MAGIC        AND nf.mail_date = qb.mail_date
# MAGIC ),
# MAGIC campaign_match AS (
# MAGIC     SELECT
# MAGIC         dat_stage.*,
# MAGIC         ch.duns AS campaign_hist_duns,
# MAGIC         ch.nf_campaignid,
# MAGIC         ch.qb_campaignid,
# MAGIC         ch.nf_accountnum,
# MAGIC         ch.qb_accountnum,
# MAGIC         ch.nf_mailable,
# MAGIC         ch.qb_mailable,
# MAGIC         ch.load_date,
# MAGIC         ch.run_date,
# MAGIC         ch.mail_date
# MAGIC     FROM dat_stage
# MAGIC     LEFT JOIN campaign_hist ch
# MAGIC     ON dat_stage.duns = ch.duns
# MAGIC     AND dat_stage.bau_campaign_match_mail_date = ch.mail_date
# MAGIC )
# MAGIC SELECT * FROM campaign_match
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC staging AS (
# MAGIC SELECT dat.*
# MAGIC  
# MAGIC     , nf_resp.proba AS nf_proba_RESP_7
# MAGIC     , nf_resp.mdl_name AS nf_mdl_name_RESP_7
# MAGIC     , nf_sub.proba AS nf_proba_SUB_7
# MAGIC     , nf_sub.mdl_name AS nf_mdl_name_SUB_7
# MAGIC     , qb_resp.proba AS qb_proba_RESP_7
# MAGIC     , qb_resp.mdl_name AS qb_mdl_name_RESP_7
# MAGIC     , qb_sub.proba AS qb_proba_SUB_7
# MAGIC     , qb_sub.mdl_name AS qb_mdl_name_SUB_7
# MAGIC     , nf_resp_6.proba AS nf_proba_RESP_6
# MAGIC     , nf_resp_6.mdl_name AS nf_mdl_name_RESP_6
# MAGIC     , nf_sub_6.proba AS nf_proba_SUB_6
# MAGIC     , nf_sub_6.mdl_name AS nf_mdl_name_SUB_6
# MAGIC     , qb_resp_6.proba AS qb_proba_RESP_6
# MAGIC     , qb_resp_6.mdl_name AS qb_mdl_name_RESP_6
# MAGIC     --, qb_sub_6.proba AS qb_proba_SUB_6
# MAGIC     --, qb_sub_6.mdl_name AS qb_mdl_name_SUB_6
# MAGIC
# MAGIC
# MAGIC     , CASE WHEN ppp_df.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp
# MAGIC
# MAGIC    
# MAGIC      ,COALESCE(nfp.flg_resp, qbp.flg_resp) AS flg_resp
# MAGIC     , COALESCE(nfp.flg_qual, qbp.flg_qual) AS flg_qual
# MAGIC     , COALESCE(nfp.flg_sub, qbp.flg_sub) AS flg_sub
# MAGIC     , COALESCE(nfp.flg_appr, qbp.flg_appr) AS flg_appr
# MAGIC     ,CASE WHEN nfp.internal_approved_amount > 0 THEN 1 
# MAGIC       WHEN qbp.internal_approved_amount > 0 THEN 1 ELSE 0 END AS flg_appr_obs
# MAGIC     , COALESCE(nfp.flg_fund, qbp.flg_fund) AS flg_fund
# MAGIC     ,CASE WHEN nfp.internal_funded_amount > 0 THEN 1  WHEN qbp.internal_funded_amount > 0 THEN 1 ELSE 0 END AS flg_fund_obs
# MAGIC     , COALESCE(nfp.fund_amt, qbp.fund_amt) AS fund_amt
# MAGIC     , COALESCE(nfp.internal_funded_amount, qbp.internal_funded_amount) AS fund_amt_obs
# MAGIC
# MAGIC
# MAGIC FROM dat 
# MAGIC    
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
# MAGIC     ON dat.duns = nf_resp.duns AND dat.bau_campaign_match_mail_date = nf_resp.mail_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
# MAGIC     ON dat.duns = nf_sub.duns  AND dat.bau_campaign_match_mail_date = nf_sub.mail_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = 'V7 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
# MAGIC     ON dat.duns = qb_resp.duns AND   dat.bau_campaign_match_mail_date = qb_resp.mail_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
# MAGIC     ON dat.duns = qb_sub.duns AND   dat.bau_campaign_match_mail_date =qb_sub.mail_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = 'V7 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp_6
# MAGIC     ON dat.duns = nf_resp_6.duns AND  dat.bau_campaign_match_mail_date = nf_resp_6.mail_date AND nf_resp_6.brand = 'NF' AND nf_resp_6.model_version = 'V6 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub_6
# MAGIC     ON dat.duns = nf_sub_6.duns AND dat.bau_campaign_match_mail_date = nf_sub_6.mail_date AND nf_sub_6.brand = 'NF' AND nf_sub_6.model_version = 'V6 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp_6
# MAGIC     ON dat.duns = qb_resp_6.duns AND dat.bau_campaign_match_mail_date = qb_resp_6.mail_date AND qb_resp_6.brand = 'QB' AND qb_resp_6.model_version = 'V6 RESP'
# MAGIC     -- LEFT JOIN nf_workarea.historical_campaign_scores qb_sub_6
# MAGIC     -- ON dat.duns = qb_sub_6.duns AND dat.bau_campaign_match_mail_date = qb_sub.mail_date AND qb_sub_6.brand = 'QB' AND qb_sub_6.model_version = 'V6 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp_df
# MAGIC     ON ppp_df.EXTERNAL_ID_MARKETING=dat.duns
# MAGIC
# MAGIC     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_nf_2025111 nfp
# MAGIC     ON dat.nf_accountnum=nfp.accountnum  AND nfp.RUN_DATE = '20251101'
# MAGIC     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_qb_2025111 qbp
# MAGIC     ON dat.qb_accountnum=qbp.accountnum AND qbp.RUN_DATE = '20251101'
# MAGIC    -- WHERE dat.mdl_name_RESP IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC staging_2 AS (
# MAGIC   select *
# MAGIC       , COALESCE(nf_proba_RESP_7, qb_proba_RESP_7) AS resp_v7
# MAGIC     ,COALESCE(nf_proba_RESP_6, qb_proba_RESP_6) AS resp_v6
# MAGIC     , COALESCE(nf_proba_SUB_7, qb_proba_SUB_7) AS sub_v7
# MAGIC   --  ,COALESCE(nf_proba_SUB_6, qb_proba_SUB_6) AS sub_v6
# MAGIC    ,nf_proba_SUB_6 AS sub_v6
# MAGIC     from staging
# MAGIC )
# MAGIC SELECT *
# MAGIC
# MAGIC     , CASE
# MAGIC     WHEN resp_v7 < 0.034114 THEN 1
# MAGIC     WHEN resp_v7 < 0.068916 THEN 2
# MAGIC     WHEN resp_v7 < 0.111961 THEN 3
# MAGIC     WHEN resp_v7 < 0.162962 THEN 4
# MAGIC     WHEN resp_v7 < 0.222099 THEN 5
# MAGIC     WHEN resp_v7 < 0.284987 THEN 6
# MAGIC     WHEN resp_v7 < 0.354618 THEN 7
# MAGIC     WHEN resp_v7 < 0.437950 THEN 8
# MAGIC     WHEN resp_v7 < 0.543222 THEN 9
# MAGIC     ELSE 10
# MAGIC   END AS v7_resp_bin
# MAGIC   , CASE 
# MAGIC     WHEN ppp = 'N' THEN CASE
# MAGIC         WHEN resp_v6 < 0.06986177294190464 THEN 1
# MAGIC         WHEN resp_v6 < 0.1339245864382257 THEN 2
# MAGIC         WHEN resp_v6 < 0.19911057548148098 THEN 3
# MAGIC         WHEN resp_v6 < 0.2648634399966438 THEN 4
# MAGIC         WHEN resp_v6 < 0.3308196007628543 THEN 5
# MAGIC         WHEN resp_v6 < 0.4020418214049535 THEN 6
# MAGIC         WHEN resp_v6 < 0.4799049832521699 THEN 7
# MAGIC         WHEN resp_v6 < 0.5674352790225993 THEN 8
# MAGIC         WHEN resp_v6 < 0.6865566088244687 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC         WHEN ppp = 'Y' THEN CASE
# MAGIC         WHEN resp_v6 < 0.09192317674472977 THEN 1
# MAGIC         WHEN resp_v6 < 0.14241300658817654 THEN 2
# MAGIC         WHEN resp_v6 < 0.19302681254587684 THEN 3
# MAGIC         WHEN resp_v6 < 0.24772050172405718 THEN 4
# MAGIC         WHEN resp_v6 < 0.3096403794311008 THEN 5
# MAGIC         WHEN resp_v6 < 0.38130716726325864 THEN 6
# MAGIC         WHEN resp_v6 < 0.46656576625518276 THEN 7
# MAGIC         WHEN resp_v6 < 0.5702527435980795 THEN 8
# MAGIC         WHEN resp_v6 < 0.7003898538311522 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC   END AS v6_resp_bin,
# MAGIC   CASE
# MAGIC     WHEN sub_v7 < 0.186932 THEN 1
# MAGIC     WHEN sub_v7 < 0.266917 THEN 2
# MAGIC     WHEN sub_v7 < 0.325710 THEN 3
# MAGIC     WHEN sub_v7 < 0.377214 THEN 4
# MAGIC     WHEN sub_v7 < 0.426287 THEN 5
# MAGIC     WHEN sub_v7 < 0.479018 THEN 6
# MAGIC     WHEN sub_v7 < 0.536608 THEN 7
# MAGIC     WHEN sub_v7 < 0.607101 THEN 8
# MAGIC     WHEN sub_v7 < 0.705895 THEN 9
# MAGIC     ELSE 10
# MAGIC END AS v7_sub_bin,
# MAGIC CASE 
# MAGIC     WHEN ppp = 'N' THEN CASE
# MAGIC         WHEN sub_v6 < 0.4003984111765917 THEN 1
# MAGIC         WHEN sub_v6 < 0.4830520207143802 THEN 2
# MAGIC         WHEN sub_v6 < 0.5389749091311864 THEN 3
# MAGIC         WHEN sub_v6 < 0.5860559515801678 THEN 4
# MAGIC         WHEN sub_v6 < 0.6304661992424985 THEN 5
# MAGIC         WHEN sub_v6 < 0.6753514420846224 THEN 6
# MAGIC         WHEN sub_v6 < 0.7227107976823965 THEN 7
# MAGIC         WHEN sub_v6 < 0.7758924349663294 THEN 8
# MAGIC         WHEN sub_v6 < 0.8256361927932095 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC         WHEN ppp = 'Y' THEN CASE
# MAGIC         WHEN sub_v6 < 0.29706895621726714 THEN 1
# MAGIC         WHEN sub_v6 < 0.3885554922757615 THEN 2
# MAGIC         WHEN sub_v6 < 0.462956220812887 THEN 3
# MAGIC         WHEN sub_v6 < 0.5217694835605138 THEN 4
# MAGIC         WHEN sub_v6 < 0.5703851279083824 THEN 5
# MAGIC         WHEN sub_v6 < 0.6102073280311974 THEN 6
# MAGIC         WHEN sub_v6 < 0.6428669435988154 THEN 7
# MAGIC         WHEN sub_v6 < 0.6702748121587252 THEN 8
# MAGIC         WHEN sub_v6 < 0.6960303488028483 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC END AS v6_sub_bin
# MAGIC
# MAGIC     FROM Staging_2)
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     v6_resp_bin,
# MAGIC     v7_resp_bin,
# MAGIC     COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS account_count,
# MAGIC     SUM(flg_resp),
# MAGIC     sum(flg_resp)/ COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS resp_rate,
# MAGIC     SUM(flg_sub),
# MAGIC     SUM(flg_sub)/sum(flg_resp) as sub_rate,
# MAGIC     SUM(flg_appr),
# MAGIC     SUM(flg_appr)/sum(flg_sub) AS appr_rate,
# MAGIC     sum(flg_appr_obs),
# MAGIC     SUM(flg_appr_obs)/sum(flg_sub) AS appr_obs_rate,
# MAGIC     sum(flg_fund),
# MAGIC     sum(flg_fund)/sum(flg_appr) as appr_to_fund_rate,
# MAGIC     sum(flg_fund)/sum(flg_appr_obs) as obs_appr_to_fund_rate,
# MAGIC     sum(flg_fund)/sum(flg_sub) as sub_to_fund_rate,
# MAGIC     sum(flg_fund)/sum(flg_resp) as resp_to_fund_rate,
# MAGIC     sum(flg_fund_obs),
# MAGIC     sum(flg_fund_obs)/sum(flg_appr) as appr_to_fund_obs_rate,
# MAGIC     sum(flg_fund_obs)/sum(flg_appr_obs) as obs_appr_to_obs_fund_rate,
# MAGIC     sum(flg_fund_obs)/sum(flg_sub) as sub_to_fund_obs_rate,
# MAGIC     sum(flg_fund_obs)/sum(flg_resp) as resp_to_fund_obs_rate,
# MAGIC     sum(fund_amt),
# MAGIC     sum(fund_amt_obs)
# MAGIC FROM final_scored_table 
# MAGIC where  COALESCE(nf_accountnum, qb_accountnum) IS NOT NULL
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1,2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  with final_scored_table AS (
# MAGIC  WITH dat as (
# MAGIC     WITH dat_stage as
# MAGIC (
# MAGIC     SELECT duns,
# MAGIC     to_date(run_date, 'yyyyMMdd') AS run_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), -1) AS load_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), 2) AS mail_date_ps,
# MAGIC     CASE WHEN run_date = '20250401' THEN to_date('2025-03-01') 
# MAGIC     WHEN run_date = '20250501' THEN to_date('2025-05-01')
# MAGIC     WHEN run_date = '20250601' THEN to_date('2025-06-01') END AS bau_campaign_match_mail_date
# MAGIC     from prescreen_jun_aug_csv
# MAGIC     ),
# MAGIC campaign_hist AS (
# MAGIC     SELECT
# MAGIC         COALESCE(nf.duns, qb.duns) AS duns,
# MAGIC         nf.campaignid AS nf_campaignid,
# MAGIC         qb.campaignid AS qb_campaignid,
# MAGIC         nf.accountnum AS nf_accountnum,
# MAGIC         qb.accountnum AS qb_accountnum,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date,
# MAGIC         COALESCE(nf.mail_date, qb.mail_date) AS mail_date,
# MAGIC         CASE WHEN nf.duns IS NOT NULL THEN TRUE END AS nf_mailable,
# MAGIC         CASE WHEN qb.duns IS NOT NULL THEN TRUE END AS qb_mailable
# MAGIC     FROM (
# MAGIC         SELECT distinct campaignid, accountnum, mail_date, duns
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_NF
# MAGIC         WHERE mail_date in ('2025-03-01','2025-05-01', '2025-06-01')
# MAGIC           AND list != 'Prescreen Remail/V6'
# MAGIC     ) nf
# MAGIC     FULL OUTER JOIN (
# MAGIC         SELECT  distinct campaignid, accountnum, mail_date, duns
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_QB
# MAGIC         WHERE   mail_date in ('2025-03-01','2025-05-01', '2025-06-01')
# MAGIC           AND (list != 'Prescreen Remail/V6' OR list IS NULL)
# MAGIC     ) qb
# MAGIC         ON nf.duns = qb.duns
# MAGIC        AND nf.mail_date = qb.mail_date
# MAGIC ),
# MAGIC campaign_match AS (
# MAGIC     SELECT
# MAGIC         dat_stage.*,
# MAGIC         ch.duns AS campaign_hist_duns,
# MAGIC         ch.nf_campaignid,
# MAGIC         ch.qb_campaignid,
# MAGIC         ch.nf_accountnum,
# MAGIC         ch.qb_accountnum,
# MAGIC         ch.nf_mailable,
# MAGIC         ch.qb_mailable,
# MAGIC         ch.load_date,
# MAGIC         ch.run_date,
# MAGIC         ch.mail_date
# MAGIC     FROM dat_stage
# MAGIC     LEFT JOIN campaign_hist ch
# MAGIC     ON dat_stage.duns = ch.duns
# MAGIC     AND dat_stage.bau_campaign_match_mail_date = ch.mail_date
# MAGIC )
# MAGIC SELECT * FROM campaign_match
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC staging AS (
# MAGIC SELECT dat.*
# MAGIC  
# MAGIC     , nf_resp.proba AS nf_proba_RESP_7
# MAGIC     , nf_resp.mdl_name AS nf_mdl_name_RESP_7
# MAGIC     , nf_sub.proba AS nf_proba_SUB_7
# MAGIC     , nf_sub.mdl_name AS nf_mdl_name_SUB_7
# MAGIC     , qb_resp.proba AS qb_proba_RESP_7
# MAGIC     , qb_resp.mdl_name AS qb_mdl_name_RESP_7
# MAGIC     , qb_sub.proba AS qb_proba_SUB_7
# MAGIC     , qb_sub.mdl_name AS qb_mdl_name_SUB_7
# MAGIC     , nf_resp_6.proba AS nf_proba_RESP_6
# MAGIC     , nf_resp_6.mdl_name AS nf_mdl_name_RESP_6
# MAGIC     , nf_sub_6.proba AS nf_proba_SUB_6
# MAGIC     , nf_sub_6.mdl_name AS nf_mdl_name_SUB_6
# MAGIC     , qb_resp_6.proba AS qb_proba_RESP_6
# MAGIC     , qb_resp_6.mdl_name AS qb_mdl_name_RESP_6
# MAGIC     --, qb_sub_6.proba AS qb_proba_SUB_6
# MAGIC     --, qb_sub_6.mdl_name AS qb_mdl_name_SUB_6
# MAGIC
# MAGIC
# MAGIC     , CASE WHEN ppp_df.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp
# MAGIC
# MAGIC      ,COALESCE(nfp.flg_resp, qbp.flg_resp) AS flg_resp
# MAGIC     , COALESCE(nfp.flg_qual, qbp.flg_qual) AS flg_qual
# MAGIC     , COALESCE(nfp.flg_sub, qbp.flg_sub) AS flg_sub
# MAGIC     , COALESCE(nfp.flg_appr, qbp.flg_appr) AS flg_appr
# MAGIC     ,CASE WHEN nfp.internal_approved_amount > 0 THEN 1 
# MAGIC       WHEN qbp.internal_approved_amount > 0 THEN 1 ELSE 0 END AS flg_appr_obs
# MAGIC     , COALESCE(nfp.flg_fund, qbp.flg_fund) AS flg_fund
# MAGIC     ,CASE WHEN nfp.internal_funded_amount > 0 THEN 1  WHEN qbp.internal_funded_amount > 0 THEN 1 ELSE 0 END AS flg_fund_obs
# MAGIC     , COALESCE(nfp.fund_amt, qbp.fund_amt) AS fund_amt
# MAGIC     , COALESCE(nfp.internal_funded_amount, qbp.internal_funded_amount) AS fund_amt_obs
# MAGIC
# MAGIC
# MAGIC FROM dat 
# MAGIC    
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
# MAGIC     ON dat.duns = nf_resp.duns AND dat.bau_campaign_match_mail_date = nf_resp.mail_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
# MAGIC     ON dat.duns = nf_sub.duns  AND dat.bau_campaign_match_mail_date = nf_sub.mail_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = 'V7 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
# MAGIC     ON dat.duns = qb_resp.duns AND   dat.bau_campaign_match_mail_date = qb_resp.mail_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
# MAGIC     ON dat.duns = qb_sub.duns AND   dat.bau_campaign_match_mail_date =qb_sub.mail_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = 'V7 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp_6
# MAGIC     ON dat.duns = nf_resp_6.duns AND  dat.bau_campaign_match_mail_date = nf_resp_6.mail_date AND nf_resp_6.brand = 'NF' AND nf_resp_6.model_version = 'V6 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub_6
# MAGIC     ON dat.duns = nf_sub_6.duns AND dat.bau_campaign_match_mail_date = nf_sub_6.mail_date AND nf_sub_6.brand = 'NF' AND nf_sub_6.model_version = 'V6 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp_6
# MAGIC     ON dat.duns = qb_resp_6.duns AND dat.bau_campaign_match_mail_date = qb_resp_6.mail_date AND qb_resp_6.brand = 'QB' AND qb_resp_6.model_version = 'V6 RESP'
# MAGIC     -- LEFT JOIN nf_workarea.historical_campaign_scores qb_sub_6
# MAGIC     -- ON dat.duns = qb_sub_6.duns AND dat.bau_campaign_match_mail_date = qb_sub.mail_date AND qb_sub_6.brand = 'QB' AND qb_sub_6.model_version = 'V6 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp_df
# MAGIC     ON ppp_df.EXTERNAL_ID_MARKETING=dat.duns
# MAGIC
# MAGIC     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_nf_2025111 nfp
# MAGIC     ON dat.nf_accountnum=nfp.accountnum  AND nfp.RUN_DATE = '20251101'
# MAGIC     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_qb_2025111 qbp
# MAGIC     ON dat.qb_accountnum=qbp.accountnum AND qbp.RUN_DATE = '20251101'
# MAGIC    -- WHERE dat.mdl_name_RESP IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC staging_2 AS (
# MAGIC   select *
# MAGIC       , COALESCE(nf_proba_RESP_7, qb_proba_RESP_7) AS resp_v7
# MAGIC     ,COALESCE(nf_proba_RESP_6, qb_proba_RESP_6) AS resp_v6
# MAGIC     , COALESCE(nf_proba_SUB_7, qb_proba_SUB_7) AS sub_v7
# MAGIC   --  ,COALESCE(nf_proba_SUB_6, qb_proba_SUB_6) AS sub_v6
# MAGIC    ,nf_proba_SUB_6 AS sub_v6
# MAGIC     from staging
# MAGIC )
# MAGIC SELECT *
# MAGIC
# MAGIC     , CASE
# MAGIC     WHEN resp_v7 < 0.034114 THEN 1
# MAGIC     WHEN resp_v7 < 0.068916 THEN 2
# MAGIC     WHEN resp_v7 < 0.111961 THEN 3
# MAGIC     WHEN resp_v7 < 0.162962 THEN 4
# MAGIC     WHEN resp_v7 < 0.222099 THEN 5
# MAGIC     WHEN resp_v7 < 0.284987 THEN 6
# MAGIC     WHEN resp_v7 < 0.354618 THEN 7
# MAGIC     WHEN resp_v7 < 0.437950 THEN 8
# MAGIC     WHEN resp_v7 < 0.543222 THEN 9
# MAGIC     ELSE 10
# MAGIC   END AS v7_resp_bin
# MAGIC   , CASE 
# MAGIC     WHEN ppp = 'N' THEN CASE
# MAGIC         WHEN resp_v6 < 0.06986177294190464 THEN 1
# MAGIC         WHEN resp_v6 < 0.1339245864382257 THEN 2
# MAGIC         WHEN resp_v6 < 0.19911057548148098 THEN 3
# MAGIC         WHEN resp_v6 < 0.2648634399966438 THEN 4
# MAGIC         WHEN resp_v6 < 0.3308196007628543 THEN 5
# MAGIC         WHEN resp_v6 < 0.4020418214049535 THEN 6
# MAGIC         WHEN resp_v6 < 0.4799049832521699 THEN 7
# MAGIC         WHEN resp_v6 < 0.5674352790225993 THEN 8
# MAGIC         WHEN resp_v6 < 0.6865566088244687 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC         WHEN ppp = 'Y' THEN CASE
# MAGIC         WHEN resp_v6 < 0.09192317674472977 THEN 1
# MAGIC         WHEN resp_v6 < 0.14241300658817654 THEN 2
# MAGIC         WHEN resp_v6 < 0.19302681254587684 THEN 3
# MAGIC         WHEN resp_v6 < 0.24772050172405718 THEN 4
# MAGIC         WHEN resp_v6 < 0.3096403794311008 THEN 5
# MAGIC         WHEN resp_v6 < 0.38130716726325864 THEN 6
# MAGIC         WHEN resp_v6 < 0.46656576625518276 THEN 7
# MAGIC         WHEN resp_v6 < 0.5702527435980795 THEN 8
# MAGIC         WHEN resp_v6 < 0.7003898538311522 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC   END AS v6_resp_bin,
# MAGIC   CASE
# MAGIC     WHEN sub_v7 < 0.186932 THEN 1
# MAGIC     WHEN sub_v7 < 0.266917 THEN 2
# MAGIC     WHEN sub_v7 < 0.325710 THEN 3
# MAGIC     WHEN sub_v7 < 0.377214 THEN 4
# MAGIC     WHEN sub_v7 < 0.426287 THEN 5
# MAGIC     WHEN sub_v7 < 0.479018 THEN 6
# MAGIC     WHEN sub_v7 < 0.536608 THEN 7
# MAGIC     WHEN sub_v7 < 0.607101 THEN 8
# MAGIC     WHEN sub_v7 < 0.705895 THEN 9
# MAGIC     ELSE 10
# MAGIC END AS v7_sub_bin,
# MAGIC CASE 
# MAGIC     WHEN ppp = 'N' THEN CASE
# MAGIC         WHEN sub_v6 < 0.4003984111765917 THEN 1
# MAGIC         WHEN sub_v6 < 0.4830520207143802 THEN 2
# MAGIC         WHEN sub_v6 < 0.5389749091311864 THEN 3
# MAGIC         WHEN sub_v6 < 0.5860559515801678 THEN 4
# MAGIC         WHEN sub_v6 < 0.6304661992424985 THEN 5
# MAGIC         WHEN sub_v6 < 0.6753514420846224 THEN 6
# MAGIC         WHEN sub_v6 < 0.7227107976823965 THEN 7
# MAGIC         WHEN sub_v6 < 0.7758924349663294 THEN 8
# MAGIC         WHEN sub_v6 < 0.8256361927932095 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC         WHEN ppp = 'Y' THEN CASE
# MAGIC         WHEN sub_v6 < 0.29706895621726714 THEN 1
# MAGIC         WHEN sub_v6 < 0.3885554922757615 THEN 2
# MAGIC         WHEN sub_v6 < 0.462956220812887 THEN 3
# MAGIC         WHEN sub_v6 < 0.5217694835605138 THEN 4
# MAGIC         WHEN sub_v6 < 0.5703851279083824 THEN 5
# MAGIC         WHEN sub_v6 < 0.6102073280311974 THEN 6
# MAGIC         WHEN sub_v6 < 0.6428669435988154 THEN 7
# MAGIC         WHEN sub_v6 < 0.6702748121587252 THEN 8
# MAGIC         WHEN sub_v6 < 0.6960303488028483 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC END AS v6_sub_bin
# MAGIC
# MAGIC     FROM Staging_2)
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     v6_sub_bin,
# MAGIC     v7_sub_bin,
# MAGIC     COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS account_count,
# MAGIC     SUM(flg_resp),
# MAGIC     sum(flg_resp)/ COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS resp_rate,
# MAGIC     SUM(flg_sub),
# MAGIC     SUM(flg_sub)/sum(flg_resp) as sub_rate,
# MAGIC     SUM(flg_appr),
# MAGIC     SUM(flg_appr)/sum(flg_sub) AS appr_rate,
# MAGIC     sum(flg_appr_obs),
# MAGIC     SUM(flg_appr_obs)/sum(flg_sub) AS appr_obs_rate,
# MAGIC     sum(flg_fund),
# MAGIC     sum(flg_fund)/sum(flg_appr) as appr_to_fund_rate,
# MAGIC     sum(flg_fund)/sum(flg_appr_obs) as obs_appr_to_fund_rate,
# MAGIC     sum(flg_fund)/sum(flg_sub) as sub_to_fund_rate,
# MAGIC     sum(flg_fund)/sum(flg_resp) as resp_to_fund_rate,
# MAGIC     sum(flg_fund_obs),
# MAGIC     sum(flg_fund_obs)/sum(flg_appr) as appr_to_fund_obs_rate,
# MAGIC     sum(flg_fund_obs)/sum(flg_appr_obs) as obs_appr_to_obs_fund_rate,
# MAGIC     sum(flg_fund_obs)/sum(flg_sub) as sub_to_fund_obs_rate,
# MAGIC     sum(flg_fund_obs)/sum(flg_resp) as resp_to_fund_obs_rate,
# MAGIC     sum(fund_amt),
# MAGIC     sum(fund_amt_obs)
# MAGIC FROM final_scored_table 
# MAGIC where  COALESCE(nf_accountnum, qb_accountnum) IS NOT NULL
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1,2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating OBS FUNDING # VS $

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRESCREEN RESP
# MAGIC  with final_scored_table AS (
# MAGIC  WITH dat as (
# MAGIC     WITH dat_stage as
# MAGIC (
# MAGIC     SELECT duns,
# MAGIC     to_date(run_date, 'yyyyMMdd') AS run_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), -1) AS load_date_ps
# MAGIC     , ADD_MONTHS(to_date(run_date,'yyyyMMdd'), 2) AS mail_date_ps,
# MAGIC     CASE WHEN run_date = '20250401' THEN to_date('2025-03-01') 
# MAGIC     WHEN run_date = '20250501' THEN to_date('2025-05-01')
# MAGIC     WHEN run_date = '20250601' THEN to_date('2025-06-01') END AS bau_campaign_match_mail_date
# MAGIC     from prescreen_jun_aug_csv
# MAGIC     ),
# MAGIC campaign_hist AS (
# MAGIC     SELECT
# MAGIC         COALESCE(nf.duns, qb.duns) AS duns,
# MAGIC         nf.campaignid AS nf_campaignid,
# MAGIC         qb.campaignid AS qb_campaignid,
# MAGIC         nf.accountnum AS nf_accountnum,
# MAGIC         qb.accountnum AS qb_accountnum,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date,
# MAGIC         ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date,
# MAGIC         COALESCE(nf.mail_date, qb.mail_date) AS mail_date,
# MAGIC         CASE WHEN nf.duns IS NOT NULL THEN TRUE END AS nf_mailable,
# MAGIC         CASE WHEN qb.duns IS NOT NULL THEN TRUE END AS qb_mailable
# MAGIC     FROM (
# MAGIC         SELECT distinct campaignid, accountnum, mail_date, duns
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_NF
# MAGIC         WHERE mail_date in ('2025-03-01','2025-05-01', '2025-06-01')
# MAGIC           AND list != 'Prescreen Remail/V6'
# MAGIC     ) nf
# MAGIC     FULL OUTER JOIN (
# MAGIC         SELECT  distinct campaignid, accountnum, mail_date, duns
# MAGIC         FROM nf_workarea.AcquisitionMailHistory_QB
# MAGIC         WHERE   mail_date in ('2025-03-01','2025-05-01', '2025-06-01')
# MAGIC           AND (list != 'Prescreen Remail/V6' OR list IS NULL)
# MAGIC     ) qb
# MAGIC         ON nf.duns = qb.duns
# MAGIC        AND nf.mail_date = qb.mail_date
# MAGIC ),
# MAGIC campaign_match AS (
# MAGIC     SELECT
# MAGIC         dat_stage.*,
# MAGIC         ch.duns AS campaign_hist_duns,
# MAGIC         ch.nf_campaignid,
# MAGIC         ch.qb_campaignid,
# MAGIC         ch.nf_accountnum,
# MAGIC         ch.qb_accountnum,
# MAGIC         ch.nf_mailable,
# MAGIC         ch.qb_mailable,
# MAGIC         ch.load_date,
# MAGIC         ch.run_date,
# MAGIC         ch.mail_date
# MAGIC     FROM dat_stage
# MAGIC     LEFT JOIN campaign_hist ch
# MAGIC     ON dat_stage.duns = ch.duns
# MAGIC     AND dat_stage.bau_campaign_match_mail_date = ch.mail_date
# MAGIC )
# MAGIC SELECT * FROM campaign_match
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC staging AS (
# MAGIC SELECT dat.*
# MAGIC  
# MAGIC     , nf_resp.proba AS nf_proba_RESP_7
# MAGIC     , nf_resp.mdl_name AS nf_mdl_name_RESP_7
# MAGIC     , nf_sub.proba AS nf_proba_SUB_7
# MAGIC     , nf_sub.mdl_name AS nf_mdl_name_SUB_7
# MAGIC     , qb_resp.proba AS qb_proba_RESP_7
# MAGIC     , qb_resp.mdl_name AS qb_mdl_name_RESP_7
# MAGIC     , qb_sub.proba AS qb_proba_SUB_7
# MAGIC     , qb_sub.mdl_name AS qb_mdl_name_SUB_7
# MAGIC     , nf_resp_6.proba AS nf_proba_RESP_6
# MAGIC     , nf_resp_6.mdl_name AS nf_mdl_name_RESP_6
# MAGIC     , nf_sub_6.proba AS nf_proba_SUB_6
# MAGIC     , nf_sub_6.mdl_name AS nf_mdl_name_SUB_6
# MAGIC     , qb_resp_6.proba AS qb_proba_RESP_6
# MAGIC     , qb_resp_6.mdl_name AS qb_mdl_name_RESP_6
# MAGIC     --, qb_sub_6.proba AS qb_proba_SUB_6
# MAGIC     --, qb_sub_6.mdl_name AS qb_mdl_name_SUB_6
# MAGIC
# MAGIC
# MAGIC     , CASE WHEN ppp_df.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp
# MAGIC
# MAGIC      ,COALESCE(nfp.flg_resp, qbp.flg_resp) AS flg_resp
# MAGIC     , COALESCE(nfp.flg_qual, qbp.flg_qual) AS flg_qual
# MAGIC     , COALESCE(nfp.flg_sub, qbp.flg_sub) AS flg_sub
# MAGIC     , COALESCE(nfp.flg_appr, qbp.flg_appr) AS flg_appr
# MAGIC     ,CASE WHEN nfp.internal_approved_amount > 0 THEN 1 
# MAGIC       WHEN qbp.internal_approved_amount > 0 THEN 1 ELSE 0 END AS flg_appr_obs
# MAGIC     , COALESCE(nfp.flg_fund, qbp.flg_fund) AS flg_fund
# MAGIC     ,CASE WHEN nfp.internal_funded_amount > 0 THEN 1  WHEN qbp.internal_funded_amount > 0 THEN 1 ELSE 0 END AS flg_fund_obs
# MAGIC     , COALESCE(nfp.fund_amt, qbp.fund_amt) AS fund_amt
# MAGIC     , COALESCE(nfp.internal_funded_amount, qbp.internal_funded_amount) AS fund_amt_obs
# MAGIC
# MAGIC FROM dat 
# MAGIC    
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
# MAGIC     ON dat.duns = nf_resp.duns AND dat.bau_campaign_match_mail_date = nf_resp.mail_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
# MAGIC     ON dat.duns = nf_sub.duns  AND dat.bau_campaign_match_mail_date = nf_sub.mail_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = 'V7 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
# MAGIC     ON dat.duns = qb_resp.duns AND   dat.bau_campaign_match_mail_date = qb_resp.mail_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
# MAGIC     ON dat.duns = qb_sub.duns AND   dat.bau_campaign_match_mail_date =qb_sub.mail_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = 'V7 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp_6
# MAGIC     ON dat.duns = nf_resp_6.duns AND  dat.bau_campaign_match_mail_date = nf_resp_6.mail_date AND nf_resp_6.brand = 'NF' AND nf_resp_6.model_version = 'V6 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub_6
# MAGIC     ON dat.duns = nf_sub_6.duns AND dat.bau_campaign_match_mail_date = nf_sub_6.mail_date AND nf_sub_6.brand = 'NF' AND nf_sub_6.model_version = 'V6 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp_6
# MAGIC     ON dat.duns = qb_resp_6.duns AND dat.bau_campaign_match_mail_date = qb_resp_6.mail_date AND qb_resp_6.brand = 'QB' AND qb_resp_6.model_version = 'V6 RESP'
# MAGIC     -- LEFT JOIN nf_workarea.historical_campaign_scores qb_sub_6
# MAGIC     -- ON dat.duns = qb_sub_6.duns AND dat.bau_campaign_match_mail_date = qb_sub.mail_date AND qb_sub_6.brand = 'QB' AND qb_sub_6.model_version = 'V6 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp_df
# MAGIC     ON ppp_df.EXTERNAL_ID_MARKETING=dat.duns
# MAGIC
# MAGIC     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_nf_2025111 nfp
# MAGIC     ON dat.nf_accountnum=nfp.accountnum  AND nfp.RUN_DATE = '20251101'
# MAGIC     LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_qb_2025111 qbp
# MAGIC     ON dat.qb_accountnum=qbp.accountnum AND qbp.RUN_DATE = '20251101'
# MAGIC    -- WHERE dat.mdl_name_RESP IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC staging_2 AS (
# MAGIC   select *
# MAGIC       , COALESCE(nf_proba_RESP_7, qb_proba_RESP_7) AS resp_v7
# MAGIC     ,COALESCE(nf_proba_RESP_6, qb_proba_RESP_6) AS resp_v6
# MAGIC     , COALESCE(nf_proba_SUB_7, qb_proba_SUB_7) AS sub_v7
# MAGIC   --  ,COALESCE(nf_proba_SUB_6, qb_proba_SUB_6) AS sub_v6
# MAGIC    ,nf_proba_SUB_6 AS sub_v6
# MAGIC     from staging
# MAGIC )
# MAGIC SELECT *
# MAGIC
# MAGIC     , CASE
# MAGIC     WHEN resp_v7 < 0.034114 THEN 1
# MAGIC     WHEN resp_v7 < 0.068916 THEN 2
# MAGIC     WHEN resp_v7 < 0.111961 THEN 3
# MAGIC     WHEN resp_v7 < 0.162962 THEN 4
# MAGIC     WHEN resp_v7 < 0.222099 THEN 5
# MAGIC     WHEN resp_v7 < 0.284987 THEN 6
# MAGIC     WHEN resp_v7 < 0.354618 THEN 7
# MAGIC     WHEN resp_v7 < 0.437950 THEN 8
# MAGIC     WHEN resp_v7 < 0.543222 THEN 9
# MAGIC     ELSE 10
# MAGIC   END AS v7_resp_bin
# MAGIC   , CASE 
# MAGIC     WHEN ppp = 'N' THEN CASE
# MAGIC         WHEN resp_v6 < 0.06986177294190464 THEN 1
# MAGIC         WHEN resp_v6 < 0.1339245864382257 THEN 2
# MAGIC         WHEN resp_v6 < 0.19911057548148098 THEN 3
# MAGIC         WHEN resp_v6 < 0.2648634399966438 THEN 4
# MAGIC         WHEN resp_v6 < 0.3308196007628543 THEN 5
# MAGIC         WHEN resp_v6 < 0.4020418214049535 THEN 6
# MAGIC         WHEN resp_v6 < 0.4799049832521699 THEN 7
# MAGIC         WHEN resp_v6 < 0.5674352790225993 THEN 8
# MAGIC         WHEN resp_v6 < 0.6865566088244687 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC         WHEN ppp = 'Y' THEN CASE
# MAGIC         WHEN resp_v6 < 0.09192317674472977 THEN 1
# MAGIC         WHEN resp_v6 < 0.14241300658817654 THEN 2
# MAGIC         WHEN resp_v6 < 0.19302681254587684 THEN 3
# MAGIC         WHEN resp_v6 < 0.24772050172405718 THEN 4
# MAGIC         WHEN resp_v6 < 0.3096403794311008 THEN 5
# MAGIC         WHEN resp_v6 < 0.38130716726325864 THEN 6
# MAGIC         WHEN resp_v6 < 0.46656576625518276 THEN 7
# MAGIC         WHEN resp_v6 < 0.5702527435980795 THEN 8
# MAGIC         WHEN resp_v6 < 0.7003898538311522 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC   END AS v6_resp_bin,
# MAGIC   CASE
# MAGIC     WHEN sub_v7 < 0.186932 THEN 1
# MAGIC     WHEN sub_v7 < 0.266917 THEN 2
# MAGIC     WHEN sub_v7 < 0.325710 THEN 3
# MAGIC     WHEN sub_v7 < 0.377214 THEN 4
# MAGIC     WHEN sub_v7 < 0.426287 THEN 5
# MAGIC     WHEN sub_v7 < 0.479018 THEN 6
# MAGIC     WHEN sub_v7 < 0.536608 THEN 7
# MAGIC     WHEN sub_v7 < 0.607101 THEN 8
# MAGIC     WHEN sub_v7 < 0.705895 THEN 9
# MAGIC     ELSE 10
# MAGIC END AS v7_sub_bin,
# MAGIC CASE 
# MAGIC     WHEN ppp = 'N' THEN CASE
# MAGIC         WHEN sub_v6 < 0.4003984111765917 THEN 1
# MAGIC         WHEN sub_v6 < 0.4830520207143802 THEN 2
# MAGIC         WHEN sub_v6 < 0.5389749091311864 THEN 3
# MAGIC         WHEN sub_v6 < 0.5860559515801678 THEN 4
# MAGIC         WHEN sub_v6 < 0.6304661992424985 THEN 5
# MAGIC         WHEN sub_v6 < 0.6753514420846224 THEN 6
# MAGIC         WHEN sub_v6 < 0.7227107976823965 THEN 7
# MAGIC         WHEN sub_v6 < 0.7758924349663294 THEN 8
# MAGIC         WHEN sub_v6 < 0.8256361927932095 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC         WHEN ppp = 'Y' THEN CASE
# MAGIC         WHEN sub_v6 < 0.29706895621726714 THEN 1
# MAGIC         WHEN sub_v6 < 0.3885554922757615 THEN 2
# MAGIC         WHEN sub_v6 < 0.462956220812887 THEN 3
# MAGIC         WHEN sub_v6 < 0.5217694835605138 THEN 4
# MAGIC         WHEN sub_v6 < 0.5703851279083824 THEN 5
# MAGIC         WHEN sub_v6 < 0.6102073280311974 THEN 6
# MAGIC         WHEN sub_v6 < 0.6428669435988154 THEN 7
# MAGIC         WHEN sub_v6 < 0.6702748121587252 THEN 8
# MAGIC         WHEN sub_v6 < 0.6960303488028483 THEN 9
# MAGIC         ELSE 10
# MAGIC     END
# MAGIC END AS v6_sub_bin
# MAGIC
# MAGIC     FROM Staging_2)
# MAGIC
# MAGIC SELECT
# MAGIC *
# MAGIC from final_scored_table where COALESCE(nf_accountnum, qb_accountnum) IS NOT NULL
# MAGIC and v6_resp_bin = 10
# MAGIC and v7_resp_bin = 3
# MAGIC and fund_amt_obs > 0
# MAGIC --     v6_resp_bin,
# MAGIC --     v7_resp_bin,
# MAGIC --     COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS account_count,
# MAGIC --     SUM(flg_resp),
# MAGIC --     sum(flg_resp)/ COUNT(DISTINCT COALESCE(nf_accountnum, qb_accountnum)) AS resp_rate,
# MAGIC --     SUM(flg_sub),
# MAGIC --     SUM(flg_sub)/sum(flg_resp) as sub_rate,
# MAGIC --     SUM(flg_appr),
# MAGIC --     SUM(flg_appr)/sum(flg_sub) AS appr_rate,
# MAGIC --     sum(flg_appr_obs),
# MAGIC --     SUM(flg_appr_obs)/sum(flg_sub) AS appr_obs_rate,
# MAGIC --     sum(flg_fund),
# MAGIC --     sum(flg_fund)/sum(flg_appr) as appr_to_fund_rate,
# MAGIC --     sum(flg_fund)/sum(flg_appr_obs) as obs_appr_to_fund_rate,
# MAGIC --     sum(flg_fund)/sum(flg_sub) as sub_to_fund_rate,
# MAGIC --     sum(flg_fund)/sum(flg_resp) as resp_to_fund_rate,
# MAGIC --     sum(flg_fund_obs),
# MAGIC --     sum(flg_fund_obs)/sum(flg_appr) as appr_to_fund_obs_rate,
# MAGIC --     sum(flg_fund_obs)/sum(flg_appr_obs) as obs_appr_to_obs_fund_rate,
# MAGIC --     sum(flg_fund_obs)/sum(flg_sub) as sub_to_fund_obs_rate,
# MAGIC --     sum(flg_fund_obs)/sum(flg_resp) as resp_to_fund_obs_rate,
# MAGIC --     sum(fund_amt),
# MAGIC --     sum(fund_amt_obs)
# MAGIC -- FROM final_scored_table 
# MAGIC -- where  COALESCE(nf_accountnum, qb_accountnum) IS NOT NULL
# MAGIC -- GROUP BY 1,2
# MAGIC -- ORDER BY 1,2
# MAGIC