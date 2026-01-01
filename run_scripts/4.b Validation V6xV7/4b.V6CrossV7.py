# Databricks notebook source
# MAGIC %sql
# MAGIC -- v6_cross_v7 = spark.sql("""
# MAGIC with final_scored_table AS (
# MAGIC   WITH dat as
# MAGIC (
# MAGIC     SELECT COALESCE(nf.duns, qb.duns) AS duns
# MAGIC     , nf.campaignid as nf_campaignid
# MAGIC     , qb.campaignid as qb_campaignid
# MAGIC     , nf.accountnum as nf_accountnum
# MAGIC     , qb.accountnum as qb_accountnum
# MAGIC     , ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date
# MAGIC     , ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date
# MAGIC     , COALESCE(nf.mail_date, qb.mail_date) AS mail_date
# MAGIC     , CASE WHEN nf.duns IS NOT NULL THEN CAST('true' AS BOOLEAN) END AS nf_mailable
# MAGIC     , CASE WHEN qb.duns IS NOT NULL THEN CAST('true' AS BOOLEAN) END AS qb_mailable
# MAGIC     FROM (
# MAGIC         (
# MAGIC             SELECT *
# MAGIC             FROM nf_workarea.AcquisitionMailHistory_NF
# MAGIC             WHERE mail_date between '2025-03-01' and '2025-08-01' --and list != 'Prescreen Remail/V6'
# MAGIC         ) nf
# MAGIC         FULL OUTER JOIN 
# MAGIC         (
# MAGIC             SELECT *
# MAGIC             FROM nf_workarea.AcquisitionMailHistory_QB
# MAGIC             WHERE mail_date between '2025-03-01' and '2025-08-01' --and (list != 'Prescreen Remail/V6' or list is null)
# MAGIC         )qb
# MAGIC         ON nf.duns = qb.duns AND nf.run_date = qb.run_date
# MAGIC     )
# MAGIC ),
# MAGIC staging AS (
# MAGIC SELECT dat.*
# MAGIC     ,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
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
# MAGIC     -- , qb_sub_6.proba AS qb_proba_SUB_6
# MAGIC     -- , qb_sub_6.mdl_name AS qb_mdl_name_SUB_6
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
# MAGIC
# MAGIC
# MAGIC
# MAGIC FROM dat 
# MAGIC     INNER JOIN us_marketing.dnb_core_id_geo_us af
# MAGIC     ON dat.duns = af.duns AND YEAR(dat.load_date) = af.load_year AND MONTH(dat.load_date) = af.load_month
# MAGIC     INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
# MAGIC     ON af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
# MAGIC     ON dat.duns = nf_resp.duns AND dat.run_date = nf_resp.run_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
# MAGIC     ON dat.duns = nf_sub.duns AND dat.run_date = nf_sub.run_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = 'V7 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
# MAGIC     ON dat.duns = qb_resp.duns AND dat.run_date = qb_resp.run_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
# MAGIC     ON dat.duns = qb_sub.duns AND dat.run_date = qb_sub.run_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = 'V7 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp_6
# MAGIC     ON dat.duns = nf_resp_6.duns AND dat.run_date = nf_resp_6.run_date AND nf_resp_6.brand = 'NF' AND nf_resp_6.model_version = 'V6 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub_6
# MAGIC     ON dat.duns = nf_sub_6.duns AND dat.run_date = nf_sub_6.run_date AND nf_sub_6.brand = 'NF' AND nf_sub_6.model_version = 'V6 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp_6
# MAGIC     ON dat.duns = qb_resp_6.duns AND dat.run_date = qb_resp_6.run_date AND qb_resp_6.brand = 'QB' AND qb_resp_6.model_version = 'V6 RESP'
# MAGIC     -- LEFT JOIN nf_workarea.historical_campaign_scores qb_sub_6
# MAGIC     -- ON dat.duns = qb_sub_6.duns AND dat.run_date = qb_sub_6.run_date AND qb_sub_6.brand = 'QB' AND qb_sub_6.model_version = 'V6 SUB'
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
# MAGIC     ,nf_proba_SUB_6 AS sub_v6
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
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1,2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v6_cross_v7 = spark.sql("""
# MAGIC with final_scored_table AS (
# MAGIC   WITH dat as
# MAGIC (
# MAGIC     SELECT COALESCE(nf.duns, qb.duns) AS duns
# MAGIC     , nf.campaignid as nf_campaignid
# MAGIC     , qb.campaignid as qb_campaignid
# MAGIC     , nf.accountnum as nf_accountnum
# MAGIC     , qb.accountnum as qb_accountnum
# MAGIC     , ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date
# MAGIC     , ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date
# MAGIC     , COALESCE(nf.mail_date, qb.mail_date) AS mail_date
# MAGIC     , CASE WHEN nf.duns IS NOT NULL THEN CAST('true' AS BOOLEAN) END AS nf_mailable
# MAGIC     , CASE WHEN qb.duns IS NOT NULL THEN CAST('true' AS BOOLEAN) END AS qb_mailable
# MAGIC     FROM (
# MAGIC         (
# MAGIC             SELECT *
# MAGIC             FROM nf_workarea.AcquisitionMailHistory_NF
# MAGIC             WHERE mail_date between '2025-03-01' and '2025-08-01' and list != 'Prescreen Remail/V6'
# MAGIC         ) nf
# MAGIC         FULL OUTER JOIN 
# MAGIC         (
# MAGIC             SELECT *
# MAGIC             FROM nf_workarea.AcquisitionMailHistory_QB
# MAGIC             WHERE mail_date between '2025-03-01' and '2025-08-01' and (list != 'Prescreen Remail/V6' or list is null)
# MAGIC         )qb
# MAGIC         ON nf.duns = qb.duns AND nf.run_date = qb.run_date
# MAGIC     )
# MAGIC ),
# MAGIC staging AS (
# MAGIC SELECT dat.*
# MAGIC     ,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
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
# MAGIC     -- , qb_sub_6.proba AS qb_proba_SUB_6
# MAGIC     -- , qb_sub_6.mdl_name AS qb_mdl_name_SUB_6
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
# MAGIC
# MAGIC
# MAGIC FROM dat 
# MAGIC     INNER JOIN us_marketing.dnb_core_id_geo_us af
# MAGIC     ON dat.duns = af.duns AND YEAR(dat.load_date) = af.load_year AND MONTH(dat.load_date) = af.load_month
# MAGIC     INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
# MAGIC     ON af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
# MAGIC     ON dat.duns = nf_resp.duns AND dat.run_date = nf_resp.run_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
# MAGIC     ON dat.duns = nf_sub.duns AND dat.run_date = nf_sub.run_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = 'V7 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
# MAGIC     ON dat.duns = qb_resp.duns AND dat.run_date = qb_resp.run_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = 'V7 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
# MAGIC     ON dat.duns = qb_sub.duns AND dat.run_date = qb_sub.run_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = 'V7 SUB'
# MAGIC
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_resp_6
# MAGIC     ON dat.duns = nf_resp_6.duns AND dat.run_date = nf_resp_6.run_date AND nf_resp_6.brand = 'NF' AND nf_resp_6.model_version = 'V6 RESP'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores nf_sub_6
# MAGIC     ON dat.duns = nf_sub_6.duns AND dat.run_date = nf_sub_6.run_date AND nf_sub_6.brand = 'NF' AND nf_sub_6.model_version = 'V6 SUB'
# MAGIC     LEFT JOIN nf_workarea.historical_campaign_scores qb_resp_6
# MAGIC     ON dat.duns = qb_resp_6.duns AND dat.run_date = qb_resp_6.run_date AND qb_resp_6.brand = 'QB' AND qb_resp_6.model_version = 'V6 RESP'
# MAGIC     -- LEFT JOIN nf_workarea.historical_campaign_scores qb_sub_6
# MAGIC     -- ON dat.duns = qb_sub_6.duns AND dat.run_date = qb_sub_6.run_date AND qb_sub_6.brand = 'QB' AND qb_sub_6.model_version = 'V6 SUB'
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
# MAGIC     ,nf_proba_SUB_6 AS sub_v6
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
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1,2;
# MAGIC

# COMMAND ----------

