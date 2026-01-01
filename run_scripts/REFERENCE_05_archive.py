
import numpy as np
import pandas as pd
from pathlib import Path
import re
import os
from config.run_env import APPEND_FILE_FOLDER, STAGING_FOLDER, SAVE_FILE_COMPRESSION, MODEL_VERSION, FROM_SUMMIT
from config.final_file import RESTRICTED_ACCOUNT_IDS
from handlers import Session
from datetime import date

if __name__ == "__main__":

    session = Session(use_log=True)
    session.log.info("Running 05_archive.py...")

    ################################################
    # Get APPEND Files from FTP
    ################################################

    session.log.info("Copying Append file from FTP server...")
    copied_files_summit = {}
    for brand in ['NF','QB','SBL']:
        if brand == 'SB':
            brand_lower = 'sbl'
        else:
            brand_lower = brand.lower()

        summit_file_pattern = fr"SUMMIT_CAMPAIGN_APPEND_{session.mail_dt.strftime('%Y-%m')}_{brand}_(\d{{2}})_{brand}_ACQ_BAU.txt"
        summit_dir = FROM_SUMMIT
        summit_destination_dir = APPEND_FILE_FOLDER / session.mail_dt.strftime('%Y/Files FROM Vendor/%Y-%m/Summit/')
        copied_files_summit[brand_lower] = session.ftp.get_files(ftp_server='nf', ftp_dir=summit_dir, file_pattern=summit_file_pattern, destination_dir=summit_destination_dir, make_destination_dir=True, expect_one_match_only=True)

    session.log.info("Pulling campaignID info for ACQUISITION data")
    campaign_output = pd.DataFrame()
    for brand in ('nf', 'qb', 'sbl'):
        campaign_salesforce = session.snowflake.read_script(
                    sql_script_path=f'sql_scripts/{brand.upper()}/get_campaign_info_table.sql',
                    script_params={
                        'mail_month':session.mail_dt.strftime('%Y%m%d')
                        }
                    )
        
        campaign_output = pd.concat([campaign_output, campaign_salesforce], ignore_index=True)
 

    ################################################
    # Read In APPEND File
    ################################################
    append = {}
    campaign_append_drop_week_dist = []
    raw_summit_file_dist = []  
    raw_summit_seed_dist= []  
    raw_summit_file_total_records = []  
    raw_summit_seed_total_records= [] 
    total_records = 0
    raw_summit_file_summaries_total_records = 0
    raw_summit_seed_summaries_total_records = 0
    
    for brand in ['nf', 'qb', 'sbl']:
        file_name = copied_files_summit[brand][0]
        clean_file_name = os.path.basename(file_name)
        append[brand] = pd.read_csv(
                filepath_or_buffer=file_name,
                sep='|',
                header=0,
                encoding='utf-8',
                quoting = 3,
                dtype = {'Unique_ID': str,
                        'ZIP': str,
                        'ZIP4': str,
                        'DPBC': str,
                        'AccountNum': str,
                        'URL': str}
                )
        
        append[brand]['SOURCE']  = append[brand]['AccountNum'].dropna().astype(str).str[:4]
        append[brand]['CAMPAIGNID'] =  append[brand]['SOURCE'].astype(int).map(dict(zip(campaign_output['Source__c'].astype(int), campaign_output['Id'])))
        
        raw_summit_file_summaries = append[brand]
        raw_summit_file_total_records.append(raw_summit_file_summaries)
        raw_summit_file_summaries_total_records += len(raw_summit_file_summaries)
        grouped_raw_summit = (
            raw_summit_file_summaries
            .groupby(['SOURCE', 'CAMPAIGNID', 'drop_week'])
            .agg(
               
                accountnum_count_mail_house_raw=('AccountNum', 'nunique')
            )
            .reset_index()
        )
     
        grouped_raw_summit['file_name'] = clean_file_name
        grouped_raw_summit['brand'] = brand
        raw_summit_file_dist.append(grouped_raw_summit)
        raw_summit_seed_summaries = append[brand][
            (append[brand]['Unique_ID'] == 'SEED') |
            ((append[brand]['Unique_ID'].str.len() != 9) & (append[brand]['Unique_ID'].str.len() != 10))
        ]
        raw_summit_seed_total_records.append(raw_summit_seed_summaries)

        raw_summit_seed_summaries_total_records += len(raw_summit_seed_summaries)
        grouped_raw_summit_seed = (
            raw_summit_seed_summaries
            
            .groupby(['SOURCE', 'CAMPAIGNID', 'drop_week'])
            .agg(
                
                accountnum_count_seed=('AccountNum', 'nunique')
            )
            .reset_index()
        )
        grouped_raw_summit_seed['file_name'] = clean_file_name
        grouped_raw_summit_seed['brand'] = brand
        raw_summit_seed_dist.append(grouped_raw_summit_seed)
       
        append[brand] = append[brand][(append[brand]['Unique_ID'].str.len() == 9) | \
                                      (append[brand]['Unique_ID'].str.len() == 10)]
        condition = append[brand]['Unique_ID'] != 'SEED'
        append[brand] = append[brand][condition]
        
        session.log.info(f"Campaign append file for brand {brand} has {append[brand].shape[0]} rows")

        if append[brand].shape[0] != append[brand]['Unique_ID'].nunique():
            raise ValueError(f"Duplicate Unique_IDs found")
        if append[brand].shape[0] != append[brand]['AccountNum'].nunique():
            raise ValueError(f"Duplicate AccountNum found")
            
        append[brand]['Unique_ID'] = append[brand]['Unique_ID'].str.zfill(9)   
        append[brand]['address'] = (
        append[brand]['Addr'].where(pd.notna(append[brand]['Addr']), '') + ' ' +
        append[brand]['SecAddr'].where(pd.notna(append[brand]['SecAddr']), '')
            ).str.strip()
        append[brand]['ZIP4'] = pd.to_numeric(append[brand]['ZIP4'], errors='coerce').fillna(pd.NA).astype('Int64')
        append[brand]['ZIP4'] = append[brand]['ZIP4'].apply(lambda x: f"{int(x):04d}" if pd.notna(x) else pd.NA).astype('string')
        append[brand]['DPBC'] = pd.to_numeric(append[brand]['DPBC'], errors='coerce').fillna(pd.NA).astype('Int64')
        append[brand]['DPBC'] = append[brand]['DPBC'].apply(lambda x: f"{int(x):02d}" if pd.notna(x) else pd.NA).astype('string')
        append[brand]['SOURCE'] = append[brand]['AccountNum'].str[:4].astype(int)
        columns_to_clean = ['ZIP', 'ZIP4', 'AccountNum', 'DPBC', 'drop_week','SOURCE']
        for col in columns_to_clean:
            append[brand][col] = append[brand][col].apply(
                lambda x: (
                    str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() and str(x).endswith('.0')
                    else (np.nan if pd.isna(x) else str(x))
                )
            )
        
        
    raw_summit_file_dist_df = pd.concat(raw_summit_file_dist, ignore_index=True)
    raw_summit_file_dist_df = raw_summit_file_dist_df[['file_name', 'brand', 'SOURCE', 'CAMPAIGNID', 'drop_week', 'accountnum_count_mail_house_raw']]
    raw_summit_seed_dist_df = pd.concat(raw_summit_seed_dist, ignore_index=True)
    raw_summit_seed_dist_df = raw_summit_seed_dist_df[['file_name', 'brand', 'SOURCE', 'CAMPAIGNID', 'drop_week', 'accountnum_count_seed']]


    ################################################
    # READ IN STAGED DATA & JOIN WITH APPEND FILE
    ################################################
    if session.running_in_prod:
        save_folder = session.mail_counts_folder
    else:
       
        STAGING_FOLDER = Path(f"//corp/nffs/Departments/Marketing/Ada/Mail Counts/{session.mail_dt.strftime('%Y')}/{session.mail_dt.strftime('%Y-%m')}/Acquisition")
        save_folder = STAGING_FOLDER
        save_folder.mkdir(parents=True, exist_ok=True)

    final_file = {}
   
    for brand in ['nf','qb','sbl']:
        file_name = copied_files_summit[brand][0]
        clean_file_name = os.path.basename(file_name)
        final_file_path = save_folder / (brand + "_final_file.parquet")
        df = pd.read_parquet(path=final_file_path, engine = 'pyarrow')
    
        df['unique_id'] =  df['unique_id'].astype(str).str.zfill(9)
        # keep correct address into from append file
        df = df[[c for c in df.columns if c not in ['address', 'city', 'state', 'zip', 'zip4', 'dpbc']]]
        df_final = df.merge(append[brand].rename(columns={'Unique_ID':'unique_id'}), on='unique_id', how='outer', indicator=True, validate='one_to_one')
    
        records_with_no_account_num_assigned = (df_final['_merge'] == 'right_only').sum()
        if records_with_no_account_num_assigned > 0:
            if session.running_in_prod:
                raise ValueError(f"{records_with_no_account_num_assigned:,} records in the {brand} append file did not merge with a record in the campaign file")
            else:
                session.log.warning(f"{records_with_no_account_num_assigned:,} {brand} records in the {brand} append file did not merge with a record in the campaign file (continuing...)")

        final_file[brand] = df_final[df_final['_merge']=='both'].copy()

        # edit AccoutNum
        final_file[brand]['AccountNum'] = final_file[brand]['AccountNum'].astype(pd.Int64Dtype()).astype(str)
        final_file[brand]['account_id'] = final_file[brand]['AccountNum'].str[7:].astype(pd.Int64Dtype())

        session.log.info(f"mapped {final_file[brand].shape[0]:,} rows successfully with {brand} append file")

        ##################################################################
        # CREATING CAMPAIGN + DROPWEEK DISTRIBUTION FOR TEAMS NOTIFICATION
        ##################################################################
       
        df = final_file[brand]
        total_records += len(df)
        grouped_campaign_append = (
            df
            .groupby(['SOURCE', 'campaign_id', 'drop'])
             .agg(
                accountnum_count_data_load=('AccountNum', 'nunique'),
                duns_count_data_load=('duns', 'nunique')
            )
            .reset_index()
        )
        grouped_campaign_append['file_name'] = clean_file_name
        grouped_campaign_append['brand'] = brand
        campaign_append_drop_week_dist.append(grouped_campaign_append)

    campaign_append_drop_week_dist_df = pd.concat(campaign_append_drop_week_dist, ignore_index=True)
    campaign_append_drop_week_dist_df = campaign_append_drop_week_dist_df[['file_name', 'brand', 'SOURCE', 'campaign_id', 'drop', 'accountnum_count_data_load', 'duns_count_data_load']]
    campaign_append_drop_week_dist_df = campaign_append_drop_week_dist_df.rename(columns={'campaign_id':'CAMPAIGNID','drop' : 'drop_week'})
    

    all_data = pd.concat([append[brand] for brand in ['nf', 'qb', 'sbl']], ignore_index=True)

    dataload_accountnum_count = all_data['AccountNum'].nunique()
    
    raw_summit_file_total_records_df = pd.concat(raw_summit_file_total_records, ignore_index=True)
    raw_summit_file_total_accountnum = raw_summit_file_total_records_df['AccountNum'].nunique()
    raw_summit_seed_total_records_df = pd.concat(raw_summit_seed_total_records, ignore_index=True)
    raw_summit_seed_total_accountnum = raw_summit_seed_total_records_df['AccountNum'].nunique()

    total_summary = pd.DataFrame([{
        'file_name': 'All_bau',
        'brand': 'All_bau',
        'SOURCE': 'All_bau',
        'CAMPAIGNID': 'All_bau',
        'drop_week': 'All_bau',
        'accountnum_count_data_load': dataload_accountnum_count,
        'duns_count_data_load': pd.NA,
        'accountnum_count_mail_house_raw': raw_summit_file_total_accountnum,
        'accountnum_count_seed': raw_summit_seed_total_accountnum
    }])


    all_data_duns = pd.concat([final_file[brand] for brand in ['nf', 'qb', 'sbl']], ignore_index=True)
    duns_count = all_data_duns['duns'].nunique()
    total_summary['duns_count_data_load'] = duns_count
    total_summary = total_summary[['file_name', 'brand', 'SOURCE', 'CAMPAIGNID', 'drop_week', 'accountnum_count_mail_house_raw','accountnum_count_seed','accountnum_count_data_load', 'duns_count_data_load']]


    key_cols = ['file_name', 'brand', 'SOURCE', 'CAMPAIGNID', 'drop_week']
    for df in [total_summary, raw_summit_file_dist_df, raw_summit_seed_dist_df, campaign_append_drop_week_dist_df]:
        df['drop_week'] = df['drop_week'].astype(str)

    merged_df = total_summary.merge(raw_summit_file_dist_df, on=key_cols, how='outer')
    merged_df = merged_df.merge(raw_summit_seed_dist_df, on=key_cols, how='outer')
    merged_df = merged_df.merge(campaign_append_drop_week_dist_df, on=key_cols, how='outer')

    columns_to_resolve = ['accountnum_count_mail_house_raw','accountnum_count_seed','accountnum_count_data_load', 'duns_count_data_load']
    for col in columns_to_resolve:
        candidates = [c for c in merged_df.columns if c.startswith(col)]
        if len(candidates) > 1:
            merged_df[col] = merged_df[candidates[0]]
            for other in candidates[1:]:
                merged_df[col] = merged_df[col].combine_first(merged_df[other])
            merged_df.drop(columns=candidates, inplace=True)
    final_columns = key_cols + [
        'accountnum_count_mail_house_raw',
        'accountnum_count_seed',
        'accountnum_count_data_load',
        'duns_count_data_load'
    ]

    missing = [col for col in final_columns if col not in merged_df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in merged_df: {missing}")

    merged_df = merged_df[final_columns]
  
    merged_df = merged_df[
    ~((merged_df.get('file_name') != 'All_bau') & (merged_df['accountnum_count_mail_house_raw'].isna()))
]
    merged_df.to_csv(f'{session.mail_dt.strftime("%Y-%m")}_bau_drop_week_dist.csv', index=False)
  

    ################################################
    # Update Counts
    ################################################
    if session.running_in_prod:
        counts_folder = session.mail_counts_folder
    else:
         STAGING_FOLDER = Path(f"//corp/nffs/Departments/Marketing/Ada/Mail Counts/{session.mail_dt.strftime('%Y')}/{session.mail_dt.strftime('%Y-%m')}/Acquisition")
         counts_folder = STAGING_FOLDER

    counts_file_path = counts_folder / "counts_campaign_append.xlsx"

    session.log.info("Writing counts file")
    writer = pd.ExcelWriter(counts_file_path)
    col_bins = {
        'sales_volume':[0, 100000, 250000, 500000, float("inf")],
        'TIB':[0, 1, 6, float("inf")],
        'emp_total':[0, 2, 5, float("inf")], 
        }

    for brand in (['nf','qb', 'sbl']):
        startnum = 0

        # prepare data
        df_ct_data = final_file[brand][final_file[brand]['list_name'].isna()==False].copy()
        df_ct_data['NLPL_flg'] = df_ct_data[f'{brand}_market_segment'].str.split('-',expand=True)[0]
        
        df_ct_data['TIB'] = session.mail_dt.year - pd.to_numeric(df_ct_data['year_started'], errors='coerce')
        for k in col_bins.keys():
            df_ct_data[f'grp_{k}'] = pd.cut(df_ct_data[k], bins=col_bins[k], right=False)
            df_ct_data[f'grp_{k}'] = df_ct_data[f'grp_{k}'].astype(str)

        # gen distribution
        for col in [['file_type','list_name'],'campaign_id','drop', 'NLPL_flg'] +  \
            ['grp_' + c for c in col_bins.keys()] + \
            ['source'] + [['source', 'grp_' + c] for c in col_bins.keys()]:

            col = col if isinstance(col, list) else [col]
            df_ct = pd.DataFrame(df_ct_data[col].value_counts(dropna=False).rename('counts').reset_index().sort_values(by=col))

            # write to excel
            if col[0]=='source': 
                if len(col) == 1:
                    startnum = 0
                else:
                    df_ct['pcnt'] = round((df_ct['counts'] / df_ct.groupby(['source'])['counts'].transform('sum'))*100, 1)
                startcolnum = 5
            else:
                startcolnum = 0

            df_ct.to_excel(writer, sheet_name=brand, startrow=startnum, startcol=startcolnum, index=False)
            startnum += len(df_ct)+2
        
        # gen summary
        df_ct_summary = df_ct_data.groupby(['source','campaign_id','file_type','list_name','drop'
                                            ,'grp_sales_volume','grp_TIB', 'grp_emp_total','NLPL_flg'], dropna=False, observed=True)['unique_id'].count()\
                                            .reset_index().rename(columns = {'unique_id':'counts'})
        df_ct_summary.to_excel(writer, sheet_name=f'Consolidation {brand}', startrow=0, startcol=0, index=False)
        
    writer.close() 
    writer.handles = None
    session.log.info("Done writing counts file")

    #############################################
    #ARCHIVE DATA FOR REPORTING
    #############################################
   
        
    for brand in ('nf','qb','sbl'):
        appended_file_path = save_folder / (brand + "_final_file_after_appending_accountnum.parquet")
    
        final_file[brand].to_parquet(path=appended_file_path, compression=SAVE_FILE_COMPRESSION, index=False, engine = 'pyarrow')
        session.log.info(f"Saved {brand} final files with {len(final_file[brand])} rows to staging folder")

        session.log.info(f"Uploading {brand.upper()} campaign append files to Snowflake server...")
        session.snowflake.execute_script('sql_scripts/update_campaign_append.sql',
                                                    script_params = {'brand': brand,
                                                                    'file_path': save_folder,
                                                                    'run_month': session.run_dt.strftime('%Y%m%d')}
                                                    )
        session.log.info(f"Finished uploading {brand.upper()} campaign append files to Snowflake server...")


    ###############################################
    #Update and exit
    ################################################

    # Send out TEAMS update
    if session.running_in_prod:
        session.teams.send_debug_message (
                f"Complete: Archive, {'PRODUCTION' if session.running_in_prod else 'Development/Testing'}, Run Date {session.run_dt.strftime('%Y-%m-%d')}, Script {__file__}"
            )
   
        #Blind seed file
        session.teams.upload_file_to_debug(
            file_path=session.mail_counts_folder / 'blind_seeds.xlsx',
            title=f'Blind Seeds'
            )
        #Log file
        session.teams.upload_file_to_debug(
           file_path=f'AMP-log-{date.today().strftime("%Y-%m-%d")}.log',
                title=f'AMP-log-{date.today().strftime("%Y-%m-%d")}.log'
        )

    session.log.info("Done with 05_archive.py")