import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta, date
from config.run_env import MAIL_COUNTS_FOLDER
from handlers import Session
from dateutil.relativedelta import relativedelta


if __name__ == "__main__":

    session = Session(use_log=True)
    session.log.info("Running 05d_final_file_creation_for_engineering.py...")
    mail_month = session.mail_dt.strftime('%Y%m')
    mail_month_sql = session.mail_dt.strftime('%Y%m%d')
    run_month_sql =   session.run_dt.strftime('%Y%m%d')
    #############################UPDATE HERE WITH WHICH FILES TO RUN 05d FOR#####################################
    ###################### Options are 'bau' (for 05a) , 'prescreen' (for 05c), 'exl' (for 05b), 'equifax' (for equifax) or whatever supplemental file there is
    files_to_add = ['exl','equifax'] ##
    ############# EDIT THIS DURING EACH MONTHLY RUN
    file_templates = {
        'bau':      f'{session.mail_dt.strftime("%Y-%m")}_bau_drop_week_dist.csv',
        'prescreen': f'{session.mail_dt.strftime("%Y-%m")}_PS_drop_week_dist.csv',
        'exl':      f'{session.mail_dt.strftime("%Y-%m")}_EXL_drop_week_dist.csv',
        'equifax':  f'{session.mail_dt.strftime("%Y-%m")}_PS_EFX_drop_week_dist.csv'
    }

    ############################################################################################################
    combined_input_files = []
    for idx, key in enumerate(files_to_add):
        filename = file_templates[key]
        if idx == 0:
            df = pd.read_csv(filename)
            df['file_type'] = key
        
        else:
            df = pd.read_csv(filename, skiprows=1, header=None)  # skip header
            df['file_type'] = key
            df.columns = combined_input_files[0].columns  # apply column names
        
        combined_input_files.append(df)
    combined_df = pd.concat(combined_input_files, ignore_index=True)
    subset_df = combined_df[combined_df['brand'].isin(['All_bau', 'All_ps','All_ps_efx','All'])]
    summed_row = subset_df.select_dtypes(include='number').sum()
    summed_row['brand'] = 'ALL_FILES_FROM_SUMMIT'
    summed_row['SOURCE'] = 'ALL_SOURCES_FROM_SUMMIT'
    combined_df = pd.concat([combined_df, pd.DataFrame([summed_row])], ignore_index=True)
    combined_df.to_csv("combined_DF_testing.csv")
    unique_sources = combined_df['SOURCE'].dropna().unique().tolist()
    source_list_sql = "(" + ", ".join(f"'{s}'" for s in unique_sources) + ")"

    ###############Create final file for engineering ######################################
    final_file = session.snowflake.read_script(
                sql_script_path=f'sql_scripts/final_dataload_file_for_engineering.sql',
                script_params={
                    'mail_month':mail_month_sql ,
                    'run_month': run_month_sql,
                    'source_list': source_list_sql
                    }
                )
    final_file_for_engineering = final_file.drop('drop_week', axis=1)
    ############################Sending the Final file to engineering folder ####################
    file_types_str = '_'.join(files_to_add)
    if session.running_in_prod:
        output_path = MAIL_COUNTS_FOLDER/session.mail_dt.strftime(f"%Y/%Y-%m/Acquisition/Acq_{file_types_str}_dataload_file_{session.mail_dt.strftime('%Y%m')}.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_file_for_engineering.to_csv(output_path, index=False)
        session.log.info("Final file created")
        
    else:
        session.log.info('Not running in prod - script will not send DEV data to Mail Counts folder.')
        final_file_for_engineering.to_csv(f"Acq_{file_types_str}_dataload_file_{session.mail_dt.strftime('%Y%m')}DEV.csv")
    ###################Getting counts of file we sent to engineering #################
    final_file['account_num_prefix'] = final_file['account_number_full'].str[:4]
    grouped = (
    final_file
    .groupby(['brand', 'account_num_prefix', 'external_campaign_id', 'drop_week'])
    .agg(
        engineering_file_total_count=('brand', 'count'),
        engineering_file_distinct_duns=('vendor_reference_id', 'nunique'),
        engineering_file_distinct_account_num=('account_number_full', 'nunique')
    )
    .reset_index()
)
    all_row = pd.DataFrame({
    'brand': ['ALL_RECORDS_TO_ENGINEERING'],
    'account_num_prefix': ['ALL_RECORDS_TO_ENGINEERING'],
    'external_campaign_id': ['ALL_RECORDS_TO_ENGINEERING'],
    'drop_week':  ['ALL_DROPS'],
    'engineering_file_total_count': [final_file.shape[0]],
    'engineering_file_distinct_duns': [final_file['vendor_reference_id'].nunique()],
    'engineering_file_distinct_account_num': [final_file['account_number_full'].nunique()]
})

    summary_df = pd.concat([grouped, all_row], ignore_index=True)
    file_name = f"TESTING_COUNTS_OF_Acq_BAU_PS_NF_QB_dataload_file_{session.mail_dt.strftime('%Y%m')}.csv"
    summary_df.to_csv(file_name, index=False)


    #JOINING TO ONE FILE
    brand_map = {
    'nf': 'NATIONAL_FUNDING',
    'qb': 'QUICK_BRIDGE'
    }

    combined_df = combined_df.rename(columns={'drop': 'drop_week'})
    combined_df['drop_week'] = pd.to_numeric(combined_df['drop_week'], errors='coerce').astype('Int64')
    summary_df['drop_week'] = pd.to_numeric(summary_df['drop_week'], errors='coerce').astype('Int64')
    combined_df['brand'] = combined_df['brand'].apply(lambda x: brand_map.get(str(x).lower(), x))
    combined_with_summary = combined_df.merge(
    summary_df,
    how='left',
    left_on=['brand', 'SOURCE', 'drop_week'],
    right_on=['brand', 'account_num_prefix', 'drop_week']
    )
    summit_row = combined_df[
    (combined_df['brand'] == 'ALL_FILES_FROM_SUMMIT') &
    (combined_df['SOURCE'] == 'ALL_SOURCES_FROM_SUMMIT')].reset_index(drop=True)
    engineering_row = summary_df[
    (summary_df['brand'] == 'ALL_RECORDS_TO_ENGINEERING') &
    (summary_df['account_num_prefix'] == 'ALL_RECORDS_TO_ENGINEERING')].reset_index(drop=True)
    engineering_row = engineering_row.drop(columns=['brand', 'account_num_prefix', 'drop_week'], errors='ignore')

    special_join_row = summit_row.join(engineering_row)

    # Append to combined_with_summary
    combined_with_summary = combined_with_summary[
    ~(
        (combined_with_summary['brand'] == 'ALL_FILES_FROM_SUMMIT') &
        (combined_with_summary['SOURCE'] == 'ALL_SOURCES_FROM_SUMMIT')
    )
]
    combined_with_summary = pd.concat([combined_with_summary, special_join_row], ignore_index=True)

    ###RAISING A FLAG IF DATA TO ENGINEERING != DATA IN APPEND (MINUS SBL)
    summit_row = combined_with_summary[combined_with_summary['brand'] == 'ALL_FILES_FROM_SUMMIT']
    summit_count = summit_row['accountnum_count_data_load'].values[0]
    engineering_count = summit_row['engineering_file_total_count'].values[0]
    sbl_sum = combined_with_summary.loc[
    combined_with_summary['brand'] == 'sbl','accountnum_count_data_load'].sum()
    expected_total = engineering_count + sbl_sum
    
    if summit_count != expected_total:
        raise ValueError(
            f"Data mismatch:\n"
            f"SUMMIT accountnum_count_data_load = {summit_count}, "
            f"but expected {engineering_count} (engineering) + {sbl_sum} (SBL) = {expected_total}"
        )

    session.log.info("Data check passed: SUMMIT total matches engineering + SBL.")
    session.log.info(f"Data check passed: SUMMIT total ({summit_count}) = engineering ({engineering_count})+ SBL ({sbl_sum}).")

    ### RAISING A FLAG IF Data from summit minus seed data != Data in append
    summit_raw_count = summit_row['accountnum_count_mail_house_raw'].values[0]
    summit_seed_count = summit_row['accountnum_count_seed'].values[0]
    summit_accountnum_data_load = summit_row['accountnum_count_data_load'].values[0]
    expected_total_2 = summit_seed_count + summit_accountnum_data_load
    
    if summit_raw_count != expected_total_2:
        raise ValueError(
            f"Data mismatch:\n"
            f"SUMMIT accountnum_count_mail_house_raw = {summit_raw_count}, "
            f"but expected {summit_seed_count} (seed) + {summit_accountnum_data_load} (summit_accountnum_data_load) = {expected_total_2}"
        )

    session.log.info(f"Data check passed: SUMMIT RAW ({summit_raw_count}) = APPEND ({summit_accountnum_data_load}) + SEED ({summit_seed_count}).")
    combined_with_summary.to_csv(f"COUNTS_OF_Acq_{file_types_str}_dataload_file_{session.mail_dt.strftime('%Y%m')}.csv")
        
    ###############################################
    #Update and exit
    ###############################################
    # Send out teams update
    if session.running_in_prod:
        session.teams.send_debug_message (
               f"Final files for {file_types_str} for mail month  {session.mail_dt.strftime('%Y-%m')} are complete! They can be found in {output_path}."
            )
         #Log file
        session.teams.upload_file_to_debug(
           file_path=f"COUNTS_OF_Acq_{file_types_str}_dataload_file_{session.mail_dt.strftime('%Y%m')}.csv",
            title=f"COUNTS_OF_Acq_{file_types_str}_dataload_file_{session.mail_dt.strftime('%Y%m')}.csv"
        )
        session.teams.upload_file_to_debug(
           file_path=f'AMP-log-{date.today().strftime("%Y-%m-%d")}.log',
                title=f'AMP-log-{date.today().strftime("%Y-%m-%d")}.log'
        )
      
   
    

    
    