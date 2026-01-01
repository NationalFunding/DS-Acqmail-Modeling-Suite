# Databricks notebook source
# MAGIC %run ./_session

# COMMAND ----------

import gc
import pandas as pd
import numpy as np

# COMMAND ----------

session = NatFunSession()

# COMMAND ----------

# MAGIC %md
# MAGIC # Identify Mailable Population

# COMMAND ----------

# MAGIC %md
# MAGIC ## data pull

# COMMAND ----------

c1 = session.query_monthly_data(class_=1)
c2 = session.query_monthly_data(class_=2)

c1_df = c1.toPandas().set_index(['duns'])
session.log.info(f"C1 has {len(c1_df): 6,} available records")

c2_df = c2.toPandas().set_index(['duns'])
session.log.info(f"C2 has {len(c2_df): 6,} available records")

dnb = {
    'c1': c1_df,
    'c2': c2_df,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## inspect if the duns is mailable

# COMMAND ----------

load_AS = session.read_in_load_AS_tabs()

# COMMAND ----------

########################
# OPT OUT LIST
########################

session.log.info("Determining opt out list...")

opt_out_count = 0
for class_ in ['c1', 'c2']:
    for brand in ['nf', 'qb']:
        dnb[class_][brand + '_opt_out'] = dnb[class_].index.isin(load_AS[f'opt_out_list_{brand}']['DUNS'].astype(float).astype(int))
        opt_out_count += sum((dnb[class_].index.isin(load_AS[f'opt_out_list_{brand}']['DUNS'].astype(float).astype(int))))
session.log.info(f"{opt_out_count} were dropped due to the suppresion rule (On opt-out list)")

# Mark records that are in the compliance suppression list as opt out.
# These are records that have invoked their CCPA rights and requested that we not contact them.
# We don't have duns numbers for these records, so we have to fuzzy match on name zip code

compliance_count = 0
for class_ in ['c1', 'c2']:
    for brand in ['nf', 'qb']:

        dnb_fuzzy_match_key = dnb[class_]['business_name'] + dnb[class_]['zip_code']
        dnb_fuzzy_match_key = session.normalize_series(series=dnb_fuzzy_match_key, stop_words=[])

        dnb[class_][brand+'_opt_out'] |= dnb_fuzzy_match_key.isin(load_AS['fuzzy_match_key']['FUZZY_MATCH_KEY'])
        compliance_count += sum((dnb_fuzzy_match_key.isin(load_AS['fuzzy_match_key']['FUZZY_MATCH_KEY'])) & (~dnb[class_].index.isin(load_AS[f'opt_out_list_{brand}']['DUNS'].astype(float).astype(int)))) # Counts were produced this way to avoid overlapping counts in two different suppressions
session.log.info(f"{compliance_count} were dropped due to the suppresion rule (No compliance business name + zip code)")

session.log.info("Done determining opt out list")

# COMMAND ----------

########################
# C2 MARKETABLE SICS (NLP MODEL)
########################

dnb["c1"]['marketable'] = True

session.log.info("Preprocessing class 2 business names...")
c2_business_name_cleaned = session.preprocess_business_names(
    business_names = dnb['c2']['business_name'].astype(pd.StringDtype()),
    use_stored_lemmatizations=True
    )

dnb["c2"]['marketable'] = session.marketable_business_classification(business_name_cleaned=c2_business_name_cleaned)
if (dnb["c2"]['marketable'].sum()/dnb["c2"]['marketable'].count() < 0.7):
    raise ValueError(f"C2 business classifier identified less than 70% of names to be marketable.")
non_marketable_count_c2 = sum((dnb["c2"]['marketable'] == False) & (dnb["c2"]['nf_opt_out'] == False) & (dnb["c2"]['qb_opt_out'] == False)) # Counts were produced this way to avoid overlapping counts in two different suppressions
session.log.info(f"{non_marketable_count_c2} were dropped due to the suppresion rule (Non Marketable SIC)")

del c2_business_name_cleaned, load_AS['opt_out_list_nf'], load_AS['opt_out_list_qb']

session.log.info("Done determining marketable SICs")

# COMMAND ----------

########################
# State & Zip Exclusion
########################

state_zip_exclusion_count = 0
for class_ in ['c1', 'c2']:
    dnb[class_]['state_zip_exclusion'] = False
    dnb[class_].loc[dnb[class_]['alph_terr_code'].isin(['WY', 'NH', 'IA', 'DC', 'AK']), 'state_zip_exclusion'] = True
    dnb[class_]['zip3'] = dnb[class_]['zip_code'].astype(str).str[:3]
    dnb[class_].loc[dnb[class_]['zip3'].isin(['331','900']), 'state_zip_exclusion'] = True
    dnb[class_].loc[dnb[class_]['zip_code'].isin(['90025','90026','90035','90021','90003']), 'state_zip_exclusion'] = True
    state_zip_exclusion_count += sum((dnb[class_]['state_zip_exclusion'] == True) & (dnb[class_]['marketable'] == True) & (dnb[class_]['nf_opt_out'] == False) & (dnb[class_]['qb_opt_out'] == False)) # Counts were produced this way to avoid overlapping counts in two different suppressions

session.log.info(f"{state_zip_exclusion_count} were dropped due to the suppresion rule (State and Zip Code Exclusion)")

session.log.info("Done determining state zip exclusion")

# COMMAND ----------

########################
# SIC2 EXCLUSION
########################

session.log.info("Excluding certain SIC2 numbers...")

sic2_exclusion_count = 0
for class_ in ['c1', 'c2']:
    dnb[class_]['sic2_excluded'] = np.where(dnb[class_]['sic2'].isin(sic2_exclusion_list), True, False)
    sic2_exclusion_count += sum((dnb[class_]['sic2_excluded'] == True) & (dnb[class_]['state_zip_exclusion'] == False) & (dnb[class_]['marketable'] == True) & (dnb[class_]['nf_opt_out'] == False) & (dnb[class_]['qb_opt_out'] == False))
session.log.info(f"{sic2_exclusion_count} were dropped due to the suppresion rule (SIC2 Exclusion)")

# COMMAND ----------

########################
# Additional Suppression (MJ Declines, Leads from other sources)
########################

session.log.info("Excluding additional suppression (MJ declines, leads from other sources) ...")

declined_duns = spark.sql("SELECT duns FROM nf_workarea.AMP_additional_suppression").toPandas()
additional_suppression_count = 0
for class_ in ['c1', 'c2']:
    dnb[class_]['misc_suppression_excluded'] = np.where(dnb[class_].index.isin(declined_duns['duns']), True, False)
    additional_suppression_count += sum((dnb[class_]['misc_suppression_excluded'] == True) & (dnb[class_]['sic2_excluded'] == False) & (dnb[class_]['state_zip_exclusion'] == False) & (dnb[class_]['marketable'] == True) & (dnb[class_]['nf_opt_out'] == False) & (dnb[class_]['qb_opt_out'] == False))
session.log.info(f"{additional_suppression_count} were dropped due to the suppresion rule (MJ declines, leads from other sources, etc.)")

# COMMAND ----------

########################
# PO Box Suppression
########################

session.log.info("Excluding businesses with PO box as their address ...")

po_box_count = 0
for class_ in ['c1', 'c2']:
    
    for col in ['mail_addr1', 'phy_addr1']:
        dnb[class_][col] = dnb[class_][col].str.lower().str.replace('\W', '', regex=True)

    po_box_index = ((dnb[class_]['mail_addr1']).str.contains('pobox') |
                    (dnb[class_]['phy_addr1']).str.contains('pobox'))
    dnb[class_]['po_box_excluded'] = np.where(po_box_index, True, False)
    po_box_count += sum((dnb[class_]['po_box_excluded'] == True) & (dnb[class_]['misc_suppression_excluded'] == False) & (dnb[class_]['sic2_excluded'] == False) & (dnb[class_]['state_zip_exclusion'] == False) & (dnb[class_]['marketable'] == True) & (dnb[class_]['nf_opt_out'] == False) & (dnb[class_]['qb_opt_out'] == False))
session.log.info(f"{po_box_count} were dropped due to the suppresion rule (PO Box as their address)")

# COMMAND ----------

########################
# CCS Points Suppression
########################

session.log.info("Excluding businesses with too low or too high CCS points ...")

ccs_suppression_count = 0
for class_ in ['c1', 'c2']:

    ccs_points_index = ((dnb[class_]['ccs_points'] <= 200) | (dnb[class_]['ccs_points'] > 650))
    dnb[class_]['ccs_excluded'] = np.where(ccs_points_index, True, False)
    ccs_suppression_count += sum((dnb[class_]['ccs_excluded'] == True) & (dnb[class_]['po_box_excluded'] == False) & (dnb[class_]['misc_suppression_excluded'] == False) & (dnb[class_]['sic2_excluded'] == False) & (dnb[class_]['state_zip_exclusion'] == False) & (dnb[class_]['marketable'] == True) & (dnb[class_]['nf_opt_out'] == False) & (dnb[class_]['qb_opt_out'] == False))
session.log.info(f"{ccs_suppression_count} were dropped due to the suppresion rule (CCS Points not btwn 200-650)")

# COMMAND ----------

########################
# NCOA Suppression
########################

session.log.info("Excluding businesses that couldn't be mailed due to NCOA suppression from the past six campaigns ...")

ncoa_suppression_count = 0
ncoa_duns = spark.sql("SELECT duns FROM nf_workarea.ncoa_duns_suppression").toPandas()
for class_ in ['c1', 'c2']:

    ncoa_points_index = (dnb[class_].index.isin(ncoa_duns['duns']))
    dnb[class_]['ncoa_excluded'] = np.where(ncoa_points_index, True, False)
    ncoa_suppression_count += sum((dnb[class_]['ncoa_excluded'] == True) & (dnb[class_]['ccs_excluded'] == False) & (dnb[class_]['po_box_excluded'] == False) & (dnb[class_]['misc_suppression_excluded'] == False) & (dnb[class_]['sic2_excluded'] == False) & (dnb[class_]['state_zip_exclusion'] == False) & (dnb[class_]['marketable'] == True) & (dnb[class_]['nf_opt_out'] == False) & (dnb[class_]['qb_opt_out'] == False))
session.log.info(f"{ncoa_suppression_count} were dropped due to being previously unable to be mailed due to NCOA")

# COMMAND ----------

########################
# MAILABLE 
########################

session.log.info("Setting mailable population...")

mailable_pop = pd.DataFrame()
for class_ in ['c1', 'c2']:
    for brand in ['nf', 'qb']:
        dnb[class_][brand + '_mailable'] = (               # a lead is mailable if:
            ~dnb[class_][brand + '_opt_out']             # have not opted out of marketing 
            &  dnb[class_]['marketable']                   # is a marketable SIC (i.e not government or non-profit)
            &  ~dnb[class_]['state_zip_exclusion']          # have not been excluded by state or zip code exclusion
            &  ~dnb[class_]['sic2_excluded']          # have not been excluded by sic exclusion
            &  ~dnb[class_]['misc_suppression_excluded']          # have not been excluded by other misc suppression
            &  ~dnb[class_]['po_box_excluded']          # have not been excluded by po box exclusion
            &  ~dnb[class_]['ccs_excluded']          # have not been excluded by ccs point suppression
            &  ~dnb[class_]['ncoa_excluded']          # have not been excluded by ccs point suppression
            )
    mailable_pop = pd.concat([mailable_pop, dnb[class_].reset_index()], axis = 0, ignore_index=True)
final_count = mailable_pop.loc[(mailable_pop['nf_mailable'] | mailable_pop['qb_mailable'])].shape[0]

# load mailable population to DB
session.load_pandas_df_to_databricks_db(df=mailable_pop.loc[(mailable_pop['nf_mailable'] | mailable_pop['qb_mailable'])
                                                            , ['duns', 'nf_mailable', 'qb_mailable']]
                                        , tab_name_in_databricks_db='mailable_pop'
                                        , write_mode = 'overwrite'
                                        , index=False)

session.log.info("Done identifying mailable population...")

# COMMAND ----------

if session.running_in_prod:
    session.suppression_impact(non_marketable_count_c2, opt_out_count, compliance_count, sic2_exclusion_count, state_zip_exclusion_count, additional_suppression_count, po_box_count, ccs_suppression_count, ncoa_suppression_count, final_count)

# COMMAND ----------

########################
# CLEAN UP
########################

del mailable_pop, dnb, c1_df, c2_df, load_AS, dnb_fuzzy_match_key
gc.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #Scoring

# COMMAND ----------

# MAGIC %skip
# MAGIC attr_for_selection = ['nf_mailable', 'qb_mailable', 'TIB', 'gc_employees', 'gc_sales', 'ccs_points', 'chg_flg', 'nf_num_times_mailed', 'qb_num_times_mailed', 'ba_cnt_cr_12m', 'inq_inquiry_duns_24m', 'ppp']
# MAGIC version = 'V50'
# MAGIC
# MAGIC ################################################
# MAGIC # Generate model data input shared with both brands
# MAGIC ################################################
# MAGIC model_data_input = session.query_model_data(model_version = version)
# MAGIC model_data_input.write.mode("overwrite").format("parquet").saveAsTable(f'{session.db}.model_data_input_{version}')
# MAGIC session.check_model_data()
# MAGIC
# MAGIC del model_data_input
# MAGIC gc.collect()
# MAGIC
# MAGIC ################################################
# MAGIC # Scoring
# MAGIC ################################################
# MAGIC
# MAGIC nfqb = {}
# MAGIC for class_ in ['c1', 'c2']:
# MAGIC
# MAGIC     for brand in ['nf', 'qb']:
# MAGIC         
# MAGIC         if brand == 'nf':
# MAGIC             
# MAGIC             final_scores_brand = pd.DataFrame()
# MAGIC             
# MAGIC             for change_ in [0, 1]:
# MAGIC
# MAGIC                 session.log.info(f"################################################################")
# MAGIC                 model_input = spark.sql(f"""SELECT * FROM {session.db}.model_data_input_{version}
# MAGIC                                             WHERE class_type = '{class_}' AND chg_flg = {change_}
# MAGIC                                             """) 
# MAGIC
# MAGIC                 pandas_input = session.conduct_brand_specific_feature_engineering(model_data=model_input)
# MAGIC                 session.log.info(f"Model dataframe has {len(pandas_input): 6,} available records")
# MAGIC                 
# MAGIC                 scored_duns = pandas_input[['duns'] + attr_for_selection] #for combining two model types: RESP & SUB
# MAGIC
# MAGIC                 for model_ in ['RESP', 'SUB']:
# MAGIC
# MAGIC                     session.batch_scoring.identify_model_list(modeling_data_source=pandas_input
# MAGIC                                                             , source_tab_brand=brand
# MAGIC                                                             , class_type=class_
# MAGIC                                                             , model_type=model_
# MAGIC                                                             , version = version
# MAGIC                                                             , ppp = NotImplemented)
# MAGIC
# MAGIC                     dffs = session.batch_scoring.score_batch(version = version)
# MAGIC
# MAGIC                     dffs.rename(columns={'proba': f'proba_{model_}'
# MAGIC                                         , 'mdl_name': f'mdl_name_{model_}'}, inplace=True)
# MAGIC                     
# MAGIC                     scored_duns = scored_duns.merge(dffs, how='left', on='duns')
# MAGIC
# MAGIC                 session.log.info(f"Done scoring the segment with {len(scored_duns): 6,} available records")
# MAGIC                 final_scores_brand = pd.concat([final_scores_brand, scored_duns], axis=0)
# MAGIC
# MAGIC             final_scores_brand.rename(columns={'proba_RESP':f'{brand}_proba_RESP'
# MAGIC                                                 ,'proba_SUB':f'{brand}_proba_SUB'
# MAGIC                                                 ,'mdl_name_RESP':f'{brand}_mdl_name_RESP'
# MAGIC                                                 ,'mdl_name_SUB':f'{brand}_mdl_name_SUB'}, inplace=True)
# MAGIC             
# MAGIC             nfqb_scores = final_scores_brand.copy()
# MAGIC
# MAGIC         else:            
# MAGIC             final_scores_brand.rename(columns={'nf_proba_RESP':f'{brand}_proba_RESP', 'nf_proba_SUB':f'{brand}_proba_SUB'
# MAGIC                                                             , 'nf_mdl_name_RESP':f'{brand}_mdl_name_RESP', 'nf_mdl_name_SUB':f'{brand}_mdl_name_SUB'}, inplace=True) 
# MAGIC                                  
# MAGIC             nfqb_scores = nfqb_scores.merge(final_scores_brand[[c for c in final_scores_brand.columns if c not in attr_for_selection]], how='outer', on='duns')
# MAGIC
# MAGIC         session.log.info(f"Done scoring brand {brand} with {len(final_scores_brand): 6,} available records")
# MAGIC         
# MAGIC     nfqb[class_] = nfqb_scores
# MAGIC     session.log.info(f"Done class {class_} scoring")
# MAGIC
# MAGIC with open(STAGING_FOLDER + f'/nfqb_{version}.pkl', 'wb') as handle:
# MAGIC     pickle.dump(nfqb, handle, protocol=pickle.HIGHEST_PROTOCOL)
# MAGIC
# MAGIC ########################
# MAGIC # CLEAN UP
# MAGIC ########################
# MAGIC
# MAGIC del final_scores_brand, scored_duns, pandas_input, dffs, nfqb_scores, model_input, nfqb
# MAGIC gc.collect()

# COMMAND ----------

duns_morgan = spark.sql("""SELECT * FROM nf_dev_workarea.model_data_input_v7 ORDER BY DUNS LIMIT 10000""").toPandas()
duns_grant = spark.sql("""SELECT * FROM nf_workarea.model_data_input ORDER BY DUNS LIMIT 10000""").toPandas()

# COMMAND ----------

merged_duns = duns_morgan.merge(duns_grant, on = 'duns')
for col in ['_'.join(x.split('_')[:-1]) for x in merged_duns.columns if '_x' in x]:
    if merged_duns[(merged_duns[col+'_x'] != merged_duns[col+'_y']) & ~(merged_duns[col+'_x'].isnull()) & ~(merged_duns[col+'_y'].isnull())].shape[0] > 0:
        print(col)

# COMMAND ----------

col = 'sic2_qtr'
merged_duns[(merged_duns[col+'_x'] != merged_duns[col+'_y']) & ~(merged_duns[col+'_x'].isnull()) & ~(merged_duns[col+'_y'].isnull())][['sic2_qtr_x', 'sic2_qtr_y']]

# COMMAND ----------

col = 'sic2_qtr'
merged_duns[(merged_duns[col+'_x'] == merged_duns[col+'_y']) & ~(merged_duns[col+'_x'].isnull()) & ~(merged_duns[col+'_y'].isnull())][['sic2_qtr_x', 'sic2_qtr_y']]

# COMMAND ----------

col = 'sic2_qtr'
merged_duns[(merged_duns[col+'_x'] != merged_duns[col+'_y']) & ~(merged_duns[col+'_x'].isnull()) & ~(merged_duns[col+'_y'].isnull())][['sic2_qtr_x', 'sic2_qtr_y']]

# COMMAND ----------

duns_morgan

# COMMAND ----------

attr_for_selection = ['nf_mailable', 'qb_mailable', 'TIB', 'gc_employees', 'gc_sales', 'ccs_points', 'chg_flg', 'nf_num_times_mailed', 'qb_num_times_mailed', 'ba_cnt_cr_12m', 'inq_inquiry_duns_24m', 'ppp']
# version = 'V6'
# attr_for_selection = ['TIB', 'via_profile', 'rdi_indicator', 'months_since_first', 'gc_sales', 'sic4', 'dnb_custom_response', 'inq_inquiry_duns_6m', 'ccs_points', 'd_sat_hq', 'sic2_qtr', 'inq_inquiry_duns_12m', 'sic2', 'less_than_truckload_buydex', 'alph_terr_code', 'inq_inquiry_duns_24m', 'inq_sic61_inq_12m', 'regular_inq_cnt', 'chg_manufacturing_ind_1m', 'nbr_ucc_filings', 'dolcur', 'via_data_depth', 'num_times_mailed', 'ba_cnt_cr_12m', 'non_fin_trade_balance', 'ba_sum_z_sic_12m', 'ba_sum_b_6m', 'totdoll_hq', 'ba_sum_f_mtch8_12m', 'tlp_score', 'layoff_score_state_percentile', 'buydex', 'advertising_buydex', 'dt_12m_hi_cr_mo_av_amt', 'less_than_truckload_buydex', 'it_buydex', 'food_bev_buydex']

version = 'V7'

################################################
# Generate model data input shared with both brands
################################################
model_data_input = session.query_model_data(model_version = version)
model_data_input.write.mode("overwrite").format("parquet").saveAsTable(f'{session.db}.model_data_input_{version}')
#session.check_model_data()

del model_data_input
gc.collect()


# COMMAND ----------

################################################
# Scoring
################################################
nfqb = {}
for class_ in ['c1', 'c2']:

    for brand in ['nf', 'qb']:
        
        if brand == 'nf':
            
            final_scores_brand = pd.DataFrame()
            
            for flg_ppp_ in ['N','Y']:

                for change_ in [0, 1]:
                    
                    session.log.info(f"################################################################")
                    model_input = spark.sql(f"""SELECT * FROM {session.db}.model_data_input_{version}
                                                WHERE class_type = '{class_}' AND ppp = '{flg_ppp_}' AND chg_flg = {change_} LIMIT 100000
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

                        dffs.rename(columns={'proba': f'proba_{model_}'
                                            , 'mdl_name': f'mdl_name_{model_}'}, inplace=True)
                        
                        scored_duns = scored_duns.merge(dffs, how='left', on='duns')

                    session.log.info(f"Done scoring the segment with {len(scored_duns): 6,} available records")
                    final_scores_brand = pd.concat([final_scores_brand, scored_duns], axis=0)

            final_scores_brand.rename(columns={'proba_RESP':f'{brand}_proba_RESP'
                                                ,'proba_SUB':f'{brand}_proba_SUB'
                                                ,'mdl_name_RESP':f'{brand}_mdl_name_RESP'
                                                ,'mdl_name_SUB':f'{brand}_mdl_name_SUB'}, inplace=True)
            
            nfqb_scores = final_scores_brand.copy()

        else:            
            final_scores_brand.rename(columns={'nf_proba_RESP':f'{brand}_proba_RESP', 'nf_proba_SUB':f'{brand}_proba_SUB'
                                                            , 'nf_mdl_name_RESP':f'{brand}_mdl_name_RESP', 'nf_mdl_name_SUB':f'{brand}_mdl_name_SUB'}, inplace=True) 
                                 
            nfqb_scores = nfqb_scores.merge(final_scores_brand[[c for c in final_scores_brand.columns if c not in attr_for_selection]], how='outer', on='duns')

        session.log.info(f"Done scoring brand {brand} with {len(final_scores_brand): 6,} available records")
        
    nfqb[class_] = nfqb_scores
    session.log.info(f"Done class {class_} scoring")

with open(STAGING_FOLDER + f'/nfqb_{version}_test_20251211.pkl', 'wb') as handle:
    pickle.dump(nfqb, handle, protocol=pickle.HIGHEST_PROTOCOL)

########################
# CLEAN UP
########################

del final_scores_brand, scored_duns, pandas_input, dffs, nfqb_scores, nfqb, model_input
gc.collect()

# COMMAND ----------

merged[merged['nf_proba_RESP_x'] != merged['nf_proba_RESP_y']].shape[0]/merged.shape[0]

# COMMAND ----------

merged[merged['nf_proba_RESP_x'] != merged['nf_proba_RESP_y']]['duns'].tolist()

# COMMAND ----------

merged[merged['nf_proba_SUB_x'] != merged['nf_proba_SUB_y']]

# COMMAND ----------

merged[merged['nf_proba_SUB_x'] != merged['nf_proba_SUB_y']].shape[0]/merged.shape[0]

# COMMAND ----------


df_c1 = nfqb_loaded["c1"]
df_c2 = nfqb_loaded["c2"]

df_all = pd.concat([df_c1, df_c2], ignore_index=True)
df_all.head()

# COMMAND ----------

df_all.shape

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

df_all["resp_score_bin"] = pd.cut(
    df_all["nf_proba_RESP"],
    bins=score_bins["V7"]["RESP"],
    include_lowest=True,
    duplicates="drop"
)


# COMMAND ----------

df_all["resp_score_bin"] = df_all["resp_score_bin"].cat.as_ordered()

# COMMAND ----------

# Count rows per bin
bin_counts = df_all["resp_score_bin"].value_counts(sort=False)

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

df_all["sub_score_bin"] = pd.cut(
    df_all["nf_proba_SUB"],
    bins=score_bins["V7"]["SUB"],
    include_lowest=True,
    duplicates="drop"
)

df_all["sub_score_bin"] = df_all["sub_score_bin"].cat.as_ordered()



# Count rows per bin
bin_counts = df_all["sub_score_bin"].value_counts(sort=False)

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

# MAGIC %md
# MAGIC # Initial Counts

# COMMAND ----------

session = NatFunSession()

# COMMAND ----------

# MAGIC %skip
# MAGIC version = 'V50'
# MAGIC col_bins = {
# MAGIC     'nf_proba_RESP': MODEL_SCOREBIN_CUTOFFS[version]['RESP'],
# MAGIC     'ccs_points': CCS_POINTS_CUTOFFS,
# MAGIC     'gc_sales_fund' : SALES_FUND_CUTOFFS,
# MAGIC     'ba_cnt_cr_12m': BA_CNT_CR_12M_CUTOFFS,
# MAGIC     'inq_inquiry_duns_24m': INQUIRY_DUNS_24M_CUTOFFS,
# MAGIC     'gc_sales_appr': SALES_APPR_CUTOFFS,
# MAGIC     'TIB' : TIB_CUTOFFS,
# MAGIC     'gc_employees': EMP_CUTOFFS,
# MAGIC     'gc_sales': SALES_FUND_CUTOFFS
# MAGIC }
# MAGIC
# MAGIC with open(STAGING_FOLDER + f'/nfqb_{version}.pkl', 'rb') as handle:
# MAGIC     nfqb = pickle.load(handle)
# MAGIC
# MAGIC for class_ in ['c1', 'c2']:
# MAGIC
# MAGIC     session.log.info(f"Starting generate initial count underline data for {class_}")
# MAGIC
# MAGIC     if class_ == 'c1':
# MAGIC         nfqb[class_]['class_type'] = 'Class 1'
# MAGIC     else:
# MAGIC         nfqb[class_]['class_type'] = 'Class 2'
# MAGIC
# MAGIC     nfqb[class_]['mc_flg'] = 'N'
# MAGIC     nfqb[class_].loc[nfqb[class_][f'chg_flg']==1, 'mc_flg'] = 'Y'
# MAGIC         
# MAGIC     for k in col_bins.keys():
# MAGIC         if k in ['gc_sales_fund', 'gc_sales_appr', 'gc_sales']:
# MAGIC             nfqb[class_][f'{k}_seg'] = (pd.cut(nfqb[class_]['gc_sales'], bins=col_bins[k], right=False).cat.codes) + 1
# MAGIC
# MAGIC         else:
# MAGIC             nfqb[class_][f'{k}_seg'] = (pd.cut(nfqb[class_][k], bins=col_bins[k], right=False).cat.codes) + 1
# MAGIC
# MAGIC         if k in ['gc_sales_fund', 'gc_sales_appr', 'ba_cnt_cr_12m', 'inq_inquiry_duns_24m', 'ccs_points']:
# MAGIC             nfqb[class_].loc[nfqb[class_][f'{k}_seg'] == 0, f'{k}_seg'] = 1
# MAGIC         elif k in ['nf_proba_RESP']:
# MAGIC             nfqb[class_]['scorebin_RESP_seg'] = 'bin 01-04'
# MAGIC             nfqb[class_].loc[nfqb[class_]['nf_proba_RESP_seg'].isin([5, 6, 7]),'scorebin_RESP_seg'] = 'bin 05-07'
# MAGIC             nfqb[class_].loc[nfqb[class_]['nf_proba_RESP_seg'].isin([8, 9, 10]),'scorebin_RESP_seg'] = 'bin 08-10'
# MAGIC     # list name
# MAGIC     nfqb[class_]['ste_seg'] = 'Sales_' + nfqb[class_]['gc_sales_seg'].astype('str') \
# MAGIC                     + '-TIB_' + nfqb[class_]['TIB_seg'].astype('str') \
# MAGIC                     + '-EMP_' + nfqb[class_]['gc_employees_seg'].astype('str')
# MAGIC
# MAGIC     nfqb[class_]['list_name'] = 'Select'
# MAGIC     for mktseg in STE_MKT_CROSSWALK.keys():
# MAGIC         nfqb[class_].loc[nfqb[class_]['ste_seg'].isin(STE_MKT_CROSSWALK[mktseg]), 'list_name'] = mktseg
# MAGIC
# MAGIC     # for the ones with no internal scores 
# MAGIC     nfqb[class_].loc[nfqb[class_]['nf_proba_RESP'].isna(), 'list_name'] = 'Material Change/Other'  
# MAGIC     
# MAGIC     session.log.info(f"Done assigning list name for {class_}")
# MAGIC
# MAGIC     for brand in ['nf', 'qb']:
# MAGIC
# MAGIC         nfqb[class_][f'{brand}_nlpl_flg'] = 'NL'
# MAGIC         nfqb[class_].loc[nfqb[class_][f'{brand}_num_times_mailed']>0, f'{brand}_nlpl_flg'] = 'PL'
# MAGIC
# MAGIC         # scorebins
# MAGIC         score_bins = MODEL_SCOREBIN_CUTOFFS[version]
# MAGIC         nfqb[class_][f'{brand}_scorebin_RESP'] = 'bin ' + (((pd.cut(nfqb[class_][f'{brand}_proba_RESP'], bins=score_bins['RESP'], right=False)).cat.codes) + 1).astype('str').str.zfill(2)
# MAGIC         nfqb[class_][f'{brand}_scorebin_SUB'] = 'bin ' + (((pd.cut(nfqb[class_][f'{brand}_proba_SUB'], bins=score_bins['SUB'], right=False)).cat.codes) + 1).astype('str').str.zfill(2)
# MAGIC         
# MAGIC         # generate rank
# MAGIC         selection_matrix_groupby = ['class_type'
# MAGIC                     , f'{brand}_nlpl_flg'
# MAGIC                     , 'list_name'
# MAGIC                     , 'ste_seg'
# MAGIC                     , 'mc_flg'
# MAGIC                     , f'{brand}_scorebin_RESP'
# MAGIC                     , f'{brand}_scorebin_SUB'
# MAGIC                     , 'gc_sales_seg'
# MAGIC                     , 'TIB_seg'
# MAGIC                     , 'gc_employees_seg'
# MAGIC                     , 'scorebin_RESP_seg'
# MAGIC                     , 'ccs_points_seg'
# MAGIC                     , 'ppp'
# MAGIC                     , 'ba_cnt_cr_12m_seg'
# MAGIC                     , 'inq_inquiry_duns_24m_seg']
# MAGIC
# MAGIC         nfqb[class_]['avg_score'] = nfqb[class_][[f'{brand}_proba_RESP', f'{brand}_proba_SUB']].mean(axis=1)
# MAGIC         nfqb[class_].loc[nfqb[class_][f'{brand}_proba_RESP'].isna(), 'avg_score'] = 0 # assign values to bin_0, otherwise no rank will assign
# MAGIC         nfqb[class_].loc[nfqb[class_][f'{brand}_mailable']==False, 'avg_score'] = np.nan # only assign rank for mailable duns
# MAGIC
# MAGIC         nfqb[class_].sort_values(by=['avg_score', 'gc_sales', 'TIB', 'gc_employees', 'duns']
# MAGIC                                     , ascending=False, inplace=True) # to be sure the result is replicable
# MAGIC         nfqb[class_][f'{brand}_rank'] = nfqb[class_].groupby(selection_matrix_groupby)['avg_score'].rank(method='first', ascending=False)
# MAGIC         nfqb[class_].drop(columns=['avg_score'], inplace=True)
# MAGIC
# MAGIC         session.log.info(f"Done assigning selection ranks for {brand}{class_}")
# MAGIC
# MAGIC     # load AMP_score_current
# MAGIC     if class_ == 'c1':
# MAGIC         mode_write = 'overwrite'
# MAGIC     else:
# MAGIC         mode_write = 'append'
# MAGIC
# MAGIC     session.load_pandas_df_to_databricks_db(df=nfqb[class_]
# MAGIC                                             , tab_name_in_databricks_db=f"AMP_score_current_{version}"
# MAGIC                                             , write_mode=mode_write, index=False)
# MAGIC     
# MAGIC ################################################
# MAGIC # Upload scores to the history tab
# MAGIC ################################################
# MAGIC session.before_loading_data_to_table(table_name = f"AMP_score_history_v5", mo_before = 12)
# MAGIC
# MAGIC for class_ in ['c1', 'c2']:
# MAGIC     nfqb[class_]['run_date'] = session.first_of_this_month.strftime('%Y-%m-%d')
# MAGIC     session.load_pandas_df_to_databricks_db(df=nfqb[class_][['duns', 'run_date'
# MAGIC                                                                     , 'nf_proba_RESP', 'nf_mdl_name_RESP'
# MAGIC                                                                     , 'nf_proba_SUB', 'nf_mdl_name_SUB'
# MAGIC                                                                     , 'qb_proba_RESP', 'qb_mdl_name_RESP'
# MAGIC                                                                     , 'qb_proba_SUB', 'qb_mdl_name_SUB'
# MAGIC                                                                     , 'nf_mailable'
# MAGIC                                                                     , 'qb_mailable'                                                        
# MAGIC                                                                     , 'class_type'
# MAGIC                                                                     , 'nf_nlpl_flg'
# MAGIC                                                                     , 'qb_nlpl_flg'
# MAGIC                                                                     , 'list_name'
# MAGIC                                                                     , 'ste_seg'
# MAGIC                                                                     , 'mc_flg'
# MAGIC                                                                     , 'nf_scorebin_RESP'
# MAGIC                                                                     , 'qb_scorebin_RESP'
# MAGIC                                                                     , 'nf_scorebin_SUB'
# MAGIC                                                                     , 'qb_scorebin_SUB'
# MAGIC                                                                     , 'gc_sales_seg'
# MAGIC                                                                     , 'TIB_seg'
# MAGIC                                                                     , 'gc_employees_seg'
# MAGIC                                                                     , 'ccs_points_seg'          
# MAGIC                                                                     ]].rename(columns = {'gc_sales_seg': 'gc_sales_seg_ctf',
# MAGIC                                                                     'TIB_seg': 'TIB_seg_ctf'})
# MAGIC                                                 , tab_name_in_databricks_db=f"AMP_score_history_v5"
# MAGIC                                                 , write_mode='append'
# MAGIC                                                 , index=False
# MAGIC                                                 , save_as_format='delta')

# COMMAND ----------

version = 'V6'
col_bins = {
    'ccs_points': CCS_POINTS_CUTOFFS,
    'gc_sales_fund' : SALES_FUND_CUTOFFS,
    'ba_cnt_cr_12m': BA_CNT_CR_12M_CUTOFFS,
    'inq_inquiry_duns_24m': INQUIRY_DUNS_24M_CUTOFFS,
    'gc_sales_appr': SALES_APPR_CUTOFFS,
    'TIB' : TIB_CUTOFFS,
    'gc_employees': EMP_CUTOFFS,
    'gc_sales': SALES_FUND_CUTOFFS
}

col_bins_for_ppp = {
    'nf_proba_RESP_NONPPP': MODEL_SCOREBIN_CUTOFFS[version]['RESP_NONPPP'],
    'nf_proba_RESP_PPP': MODEL_SCOREBIN_CUTOFFS[version]['RESP_PPP'],
}

with open(STAGING_FOLDER + f'/nfqb_{version}.pkl', 'rb') as handle:
    nfqb = pickle.load(handle)

################################################
# Create amp_score_current tab
################################################

for class_ in ['c1', 'c2']:

    session.log.info(f"Starting generate initial count underline data for {class_}")

    if class_ == 'c1':
        nfqb[class_]['class_type'] = 'Class 1'
    else:
        nfqb[class_]['class_type'] = 'Class 2'

    nfqb[class_]['mc_flg'] = 'N'
    nfqb[class_].loc[nfqb[class_][f'chg_flg']==1, 'mc_flg'] = 'Y'
        
    for k in col_bins.keys():
        if k in ['gc_sales_fund', 'gc_sales_appr']:
            nfqb[class_][f'{k}_seg'] = (pd.cut(nfqb[class_]['gc_sales'], bins=col_bins[k], right=False).cat.codes) + 1
        else:
            nfqb[class_][f'{k}_seg'] = (pd.cut(nfqb[class_][k], bins=col_bins[k], right=False).cat.codes) + 1

        if k in ['gc_sales_fund', 'gc_sales_appr', 'ba_cnt_cr_12m', 'inq_inquiry_duns_24m', 'ccs_points']:
            nfqb[class_].loc[nfqb[class_][f'{k}_seg'] == 0, f'{k}_seg'] = 1

    # populate corresponding scorebins for ppp/nonppp
    for k in col_bins_for_ppp.keys():
        nfqb[class_][f'{k}_seg'] = (pd.cut(nfqb[class_]['nf_proba_RESP'], bins=col_bins_for_ppp[k], right=False).cat.codes) + 1

    nfqb[class_]['nf_proba_RESP_seg'] = nfqb[class_]['nf_proba_RESP_NONPPP_seg']
    nfqb[class_].loc[nfqb[class_]['nf_mdl_name_RESP'].str.contains(f'_ppp'), 'nf_proba_RESP_seg'] =  nfqb[class_].loc[nfqb[class_]['nf_mdl_name_RESP'].str.contains(f'_ppp'), 'nf_proba_RESP_PPP_seg']

    nfqb[class_]['scorebin_RESP_seg'] = 'bin 01-04'
    nfqb[class_].loc[nfqb[class_]['nf_proba_RESP_seg'].isin([5, 6, 7]),'scorebin_RESP_seg'] = 'bin 05-07'
    nfqb[class_].loc[nfqb[class_]['nf_proba_RESP_seg'].isin([8, 9, 10]),'scorebin_RESP_seg'] = 'bin 08-10'

    # list name
    nfqb[class_]['ste_seg'] = 'Sales_' + nfqb[class_]['gc_sales_seg'].astype('str') \
                    + '-TIB_' + nfqb[class_]['TIB_seg'].astype('str') \
                    + '-EMP_' + nfqb[class_]['gc_employees_seg'].astype('str')

    nfqb[class_]['list_name'] = 'Select'
    for mktseg in STE_MKT_CROSSWALK.keys():
        nfqb[class_].loc[nfqb[class_]['ste_seg'].isin(STE_MKT_CROSSWALK[mktseg]), 'list_name'] = mktseg

    # for the ones with no internal scores 
    nfqb[class_].loc[nfqb[class_]['nf_proba_RESP'].isna(), 'list_name'] = 'Material Change/Other'  
    
    session.log.info(f"Done assigning list name for {class_}")

    for brand in ['nf', 'qb']:

        nfqb[class_][f'{brand}_nlpl_flg'] = 'NL'
        nfqb[class_].loc[nfqb[class_][f'{brand}_num_times_mailed']>0, f'{brand}_nlpl_flg'] = 'PL'

        # scorebins
        score_bins = MODEL_SCOREBIN_CUTOFFS[version]
        # nonppp
        nfqb[class_][f'{brand}_scorebin_RESP_NONPPP'] = 'bin ' + (((pd.cut(nfqb[class_][f'{brand}_proba_RESP'], bins=score_bins['RESP_NONPPP'], right=False)).cat.codes) + 1).astype('str').str.zfill(2)
        nfqb[class_][f'{brand}_scorebin_SUB_NONPPP'] = 'bin ' + (((pd.cut(nfqb[class_][f'{brand}_proba_SUB'], bins=score_bins['SUB_NONPPP'], right=False)).cat.codes) + 1).astype('str').str.zfill(2)
        # ppp
        nfqb[class_][f'{brand}_scorebin_RESP_PPP'] = 'bin ' + (((pd.cut(nfqb[class_][f'{brand}_proba_RESP'], bins=score_bins['RESP_PPP'], right=False)).cat.codes) + 1).astype('str').str.zfill(2)
        nfqb[class_][f'{brand}_scorebin_SUB_PPP'] = 'bin ' + (((pd.cut(nfqb[class_][f'{brand}_proba_SUB'], bins=score_bins['SUB_PPP'], right=False)).cat.codes) + 1).astype('str').str.zfill(2)
        # combine
        mask = nfqb[class_][f'{brand}_mdl_name_RESP'].str.contains('_ppp')
        nfqb[class_][f'{brand}_scorebin_RESP'] = nfqb[class_][f'{brand}_scorebin_RESP_NONPPP']
        nfqb[class_].loc[mask, f'{brand}_scorebin_RESP'] = nfqb[class_].loc[mask, f'{brand}_scorebin_RESP_PPP']
        nfqb[class_][f'{brand}_scorebin_SUB'] = nfqb[class_][f'{brand}_scorebin_SUB_NONPPP']
        nfqb[class_].loc[mask, f'{brand}_scorebin_SUB'] = nfqb[class_].loc[mask, f'{brand}_scorebin_SUB_PPP']
        
        # generate rank
        selection_matrix_groupby = ['class_type'
                    , f'{brand}_nlpl_flg'
                    , 'list_name'
                    , 'ste_seg'
                    , 'mc_flg'
                    , f'{brand}_scorebin_RESP'
                    , f'{brand}_scorebin_SUB'
                    , 'gc_sales_seg'
                    , 'TIB_seg'
                    , 'gc_employees_seg'
                    , 'scorebin_RESP_seg'
                    , 'ccs_points_seg'
                    , 'ppp'
                    , 'ba_cnt_cr_12m_seg'
                    , 'inq_inquiry_duns_24m_seg']

        nfqb[class_]['avg_score'] = nfqb[class_][[f'{brand}_proba_RESP', f'{brand}_proba_SUB']].mean(axis=1)
        nfqb[class_].loc[nfqb[class_][f'{brand}_proba_RESP'].isna(), 'avg_score'] = 0 # assign values to bin_0, otherwise no rank will assign
        nfqb[class_].loc[nfqb[class_][f'{brand}_mailable']==False, 'avg_score'] = np.nan # only assign rank for mailable duns

        nfqb[class_].sort_values(by=['avg_score', 'gc_sales', 'TIB', 'gc_employees', 'duns']
                                    , ascending=False, inplace=True) # to be sure the result is replicable
        nfqb[class_][f'{brand}_rank'] = nfqb[class_].groupby(selection_matrix_groupby)['avg_score'].rank(method='first', ascending=False)
        nfqb[class_].drop(columns=['avg_score'], inplace=True)

        session.log.info(f"Done assigning selection ranks for {brand}{class_}")

    # load AMP_score_current
    if class_ == 'c1':
        mode_write = 'overwrite'
    else:
        mode_write = 'append'

    session.load_pandas_df_to_databricks_db(df=nfqb[class_]
                                            , tab_name_in_databricks_db=f"AMP_score_current_{version}"
                                            , write_mode=mode_write, index=False)
    
################################################
# Upload scores to the history tab
################################################
session.before_loading_data_to_table(table_name = f"AMP_score_history_{version}", mo_before = 12)

for class_ in ['c1', 'c2']:
    nfqb[class_]['run_date'] = session.first_of_this_month.strftime('%Y-%m-%d')
    session.load_pandas_df_to_databricks_db(df=nfqb[class_][['duns', 'run_date'
                                                                    , 'nf_proba_RESP', 'nf_mdl_name_RESP'
                                                                    , 'nf_proba_SUB', 'nf_mdl_name_SUB'
                                                                    , 'qb_proba_RESP', 'qb_mdl_name_RESP'
                                                                    , 'qb_proba_SUB', 'qb_mdl_name_SUB'
                                                                    , 'nf_mailable'
                                                                    , 'qb_mailable'
                                                                    , 'class_type'
                                                                    , 'nf_nlpl_flg'
                                                                    , 'qb_nlpl_flg'
                                                                    , 'list_name'
                                                                    , 'ste_seg'
                                                                    , 'mc_flg'
                                                                    ,'nf_scorebin_RESP'
                                                                    , 'qb_scorebin_RESP'
                                                                    , 'nf_scorebin_SUB'
                                                                    , 'qb_scorebin_SUB'
                                                                    , 'gc_sales_seg'
                                                                    , 'TIB_seg'
                                                                    , 'gc_employees_seg'
                                                                    , 'ccs_points_seg','ppp'
                                                                    , 'ba_cnt_cr_12m_seg'
                                                                    , 'inq_inquiry_duns_24m_seg'    
                                                                    ]]
                                                , tab_name_in_databricks_db=f"AMP_score_history_{version}"
                                                , write_mode='append'
                                                , index=False
                                                , save_as_format='delta')
    session.log.info("Finished uploading scores to the history tab")