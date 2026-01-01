# Databricks notebook source
# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_sql_query

# COMMAND ----------

# MAGIC %run ./library/BatchScoring

# COMMAND ----------

from datetime import datetime
class Logger:


    def __init__(self, ts_fmt = '%Y-%m-%d %H:%M:%S'):
        """
        Dummy logger class
        """
        self.ts_fmt = ts_fmt
    
    def get_ts(self):
        
        return datetime.now().strftime(self.ts_fmt)
    
    def log(self, level, message):
        
        """
        Log a message at a specific level
        """
        
        print(self.get_ts().ljust(20), level.ljust(10), message)
        
    def info(self, message):
        
        self.log(level='INFO', message=message)


# COMMAND ----------

import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
import pandas as pd
from pathlib import Path
import pickle

import nltk
from operator import add
from functools import reduce
from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

class NatFunSession:
    
    def __init__(self):
        
        self.running_in_prod = RUN_IN_PROD
        
        self._initialize_date_information()
        
        self.log = Logger()
            
        self.batch_scoring = BatchScoring(self)

    def month_to_quarter(self, date):
        qtr = (date.month - 1) // 3 + 1
        if qtr == 1:
            qtr_month = 12
            qtr_yr = date.year - 1
        elif qtr == 2:
            qtr_month = 3
            qtr_yr = date.year
        elif qtr == 3:
            qtr_month = 6
            qtr_yr = date.year
        elif qtr in 4:
            qtr_month = 9
            qtr_yr = date.year

        return qtr_month, qtr_yr

    def _initialize_date_information(self):

        if (ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE or not self.running_in_prod) and RUN_DATE_OVERRIDE is not None:
            self.run_dt = datetime.strptime(RUN_DATE_OVERRIDE, '%Y-%m-%d')
        elif self.running_in_prod and RUN_DATE_OVERRIDE and not ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE:
            self.log_and_raise(ValueError(f"Cannot override run date in production environment without ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE set to True in the config/session.py file."))
        else:
            self.run_dt = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if self.running_in_prod:
            self.db = 'nf_workarea'
            self.output_path = INTAKE_FOLDER
        else:
            self.db = 'nf_dev_workarea'
            self.output_path = DEV_FOLDER
        
        self.first_of_this_month = self.run_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        self.mail_dt = self.first_of_this_month + relativedelta(months=2)
        self.load_dt = self.first_of_this_month + relativedelta(months=-1)
        self.load_dt_prior = self.first_of_this_month + relativedelta(months=-2)
        self.mail_date_str = self.mail_dt.strftime('%Y-%m-%d')
        self.run_year_month = self.run_dt.strftime('%Y-%m')
        self.tib_suppression_date = self.mail_dt + relativedelta(months=-12)
        # Getting load month quarter
        rem = self.load_dt.month % 3
        months_back = rem if rem != 0 else 0
        closest_qtr_dt = self.load_dt - relativedelta(months=months_back)
        self.lyr_qtr = closest_qtr_dt.year
        self.lmon_qtr = closest_qtr_dt.month

        
        # set spark config params for querying
        spark.conf.set('params.run_year', self.run_dt.year)
        spark.conf.set('params.run_month', self.run_dt.month)
        spark.conf.set('params.lyr', self.load_dt.year)
        spark.conf.set('params.lmon', self.load_dt.month)

    def load_pandas_df_to_databricks_db(self, df, tab_name_in_databricks_db, write_mode='append', index=True, save_as_format='parquet'):
        # save to parquet
        path = STAGING_FOLDER
        dbutils.fs.mkdirs(path.replace("/dbfs", ""))
        filename = tab_name_in_databricks_db
        pd.DataFrame(df).to_parquet(f'{path}/{filename}.parquet', index=index)
        self.load_parquet_to_databricks_db(parquet=f'{path}/{filename}.parquet'
                                            , tab_name_in_databricks_db=tab_name_in_databricks_db
                                            , remove_parquet=True
                                            , write_mode=write_mode
                                            , save_as_format=save_as_format)
    
    def load_parquet_to_databricks_db(self, parquet, tab_name_in_databricks_db, remove_parquet = False, write_mode='append', save_as_format='parquet'):
        parquet = parquet.replace("/dbfs", "")
        # load to DB
        sparkdf = spark.read.parquet(parquet)
        if write_mode == 'append':
            sparkdf.write.mode(write_mode).format(save_as_format).saveAsTable(f'{self.db}.{tab_name_in_databricks_db}')
        else:
            sparkdf.write.mode(write_mode).format(save_as_format).option("overwriteSchema", "true").saveAsTable(f'{self.db}.{tab_name_in_databricks_db}')

        self.log.info(f'Finished loading Tab {self.db} {tab_name_in_databricks_db} with {sparkdf.count(): 6,} rows')
        
        if remove_parquet:
            # remove the parquet file
            dbutils.fs.rm(parquet, True)
        
    def load_AS_tabs_to_databricks_db(self):
        # read in load_AS tabs to a dict
        path = INTAKE_FOLDER + f'/{self.run_year_month}'
        for filename in [filename for filename in os.listdir(path) if filename.endswith('.parquet')]:
            
            # remove 'load_as_' from campaign performance table name
            if 'df_campaign_performance_' in filename:
                tab_name = filename.replace('load_as_', '').replace('.parquet', '')
            else:
                tab_name = filename.replace('.parquet', '')
            
            self.load_parquet_to_databricks_db(parquet = f'{path}/{filename}'
                                            , tab_name_in_databricks_db = tab_name
                                            , remove_parquet = False
                                            , write_mode='overwrite'
                                            , save_as_format='parquet')
            
    def read_in_load_AS_tabs(self):
        # read in load_AS tabs to a dict
        load_AS_dict = {}
        load_AS_list = {'load_as_fuzzy_match_key': 'FUZZY_MATCH_KEY',
                        'load_as_opt_out_list_nf': 'DUNS',
                        'load_as_opt_out_list_qb': 'DUNS'}

        for load_as_tab, column_name in load_AS_list.items():
            tabname = load_as_tab.replace('load_as_', '')
            load_AS_dict[tabname] = spark.sql(f"""select {column_name} from nationalfunding_sf_share.{load_as_tab} WHERE run_date = '{self.run_dt.strftime('%Y%m%d')}'
                    """).toPandas()
            
            self.log.info(f'Finished reading in nationalfunding_sf_share.{load_as_tab} with {len(load_AS_dict[tabname]): 6,} rows.')

        return load_AS_dict

    def before_inserting_into_acquisitionmailhistory(self):
        """
        clean up duplicates
        """
        for brand in ['nf', 'qb']:
            spark.sql(f"""DELETE FROM nf_workarea.acquisitionmailhistory_{brand}
                        WHERE mail_date = '{self.first_of_this_month.strftime("%Y-%m-%d")}'""")
            self.log.info(f'Done deleting duplicates in Tab nf_workarea.acquisitionmailhistory_{brand}')

    def insert_into_acquisitionmailhistory(self, input_brand):
        query_str = INSERT_INTO_ACQUISITIONMAILHISTORY_QUERY
        query = query_str.format(brand=input_brand,
                                 mail_date = self.run_dt.strftime('%Y-%m-%d'))                                  
        df = spark.sql(query)  
        self.log.info(f"Inserted load_as_mail_history_2_months_ago_{input_brand} into nf_workarea.acquisitionmailhistory_{input_brand}")

    def get_most_recent_dnb_score_table_name(self, class_):

        if class_ not in [1, 2]:
            raise ValueError('Class arg must be 1 or 2')

        tables = spark.sql("show tables in nf_workarea").toPandas()
        custom_score_tables = tables[tables['tableName'].str.contains(rf'\d{{6}}_class{class_}_tpa')].copy()
        custom_score_tables['upload_month'] = \
            custom_score_tables['tableName']\
               .str.extract(rf'^(\d{{6}})_class{class_}_tpa')\
               .astype(int)
        custom_score_tables = custom_score_tables.sort_values(by='upload_month', ascending=False)
        most_recent = custom_score_tables.iloc[0]['tableName']
               
        return most_recent
    
    def get_most_recent_marketable_business_model(self):
        tables = pd.DataFrame(os.listdir(INTAKE_FOLDER), columns=['modelName'])
        business_model_name_tables = tables[tables['modelName'].str.contains("marketable_business_classifier")].copy()

        business_model_name_tables['version'] = \
            business_model_name_tables['modelName']\
                .str.extract(rf'marketable_business_classifier_v(\d+)-0-0.pkl')\
                .fillna(-99).astype(int)
        business_model_name_tables = business_model_name_tables.sort_values(by='version', ascending=False)
        most_recent = business_model_name_tables.iloc[0]['modelName']
               
        return INTAKE_FOLDER + '/' + most_recent
    
    def query_monthly_data(self, class_):
        
        query_str = QUERY_STR[class_]
        
        query = query_str.format(
            lyr = self.load_dt.year,
            lmon = self.load_dt.month,
            db = self.db,
            run_date = self.run_dt.strftime("%Y%m%d")
        )
        
        sdf = spark.sql(query)      
        
        return sdf
    
    def percentage_change_warning(self, current, previous, reason=None):
        if current == previous:
            pct_change = 0
        else:
            try:
                pct_change = (abs(current - previous) / previous)
            except ZeroDivisionError:
                pct_change = float('inf')

        if pct_change >= 0.05:
            return self.log.info(f"WARNING: A SIGNIFICANT DATA CHANGE WAS NOTICED FOR THE FOLLOWING REASON: {reason}. Last month's count was {previous}, but now it's {current}")
    
    def suppression_impact(self, non_marketable_count_c2, opt_out_count, compliance_count, sic2_exclusion_count, state_zip_exclusion_count, additional_suppression_count, po_box_count, ccs_suppression_count, ncoa_suppression_count, final_count):

        self.before_loading_data_to_table("suppression_count_history")

        previous_date = self.load_dt.strftime('%Y-%m-%d')
        previous_df = spark.sql(f"SELECT * FROM nf_workarea.suppression_count_history WHERE run_date = '{previous_date}'").toPandas()
        suppression_df = pd.DataFrame(columns=['Month', 'Reason', 'Count'])

        base_query = f"""
                     SELECT COUNT(DISTINCT af.duns)
                     FROM 
                    us_marketing.dnb_core_id_geo_us af
                    INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
                    ON af.load_year = {self.load_dt.year} AND af.load_month = {self.load_dt.month}
                        AND af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month
                        
                    LEFT JOIN nationalfunding_sf_share.load_as_marketable_sic_c1 marketable_sic_c1
                    ON b.sic4_primary_cd = marketable_sic_c1.sic_4 AND marketable_sic_c1.run_date = '{self.run_dt.strftime("%Y%m%d")}'
                    """
        count = spark.sql(base_query).collect()[0][0]

        suppression_df = suppression_df.append({'Reason': 'Baseline Mailable Population', 'Count': count}, ignore_index=True)
        self.log.info(f"{count} is the count of baseline mailable population for {self.run_year_month}")

        suppression_reasons = {'Activated leads for NF': ACTIVATED_LEAD_FOR_NF.format(run_date = self.run_dt.strftime("%Y%m%d")),
                               'Activated leads for QB': ACTIVATED_LEAD_FOR_QB.format(run_date = self.run_dt.strftime("%Y%m%d")),
                               'Non US territory': ONLY_US_STATES_QUERY,
                               'Empty business name': NO_NULL_BUSINESS_QUERY,
                               'Empty address': NO_EMPTY_ADDRESS_QUERY,
                               'Inactive business': ACTIVE_BUSINESS_QUERY,
                               'Non headquarter': MAIN_LOCATION_QUERY,
                               'Non positive marketability index for C1': MARKETABILITY_C1_QUERY}
        
        for reason, query in suppression_reasons.items():
            if reason != 'Non positive marketability index for C1':
                base_query += query

                filtered_count = spark.sql(base_query).collect()[0][0]
                suppression_df = suppression_df.append({'Reason': reason, 'Count': count-filtered_count}, ignore_index=True)

                if previous_df[previous_df['Reason'] == reason].shape[0] != 0:
                    previous_count = previous_df[previous_df['Reason'] == reason]['Count'].item()
                    self.percentage_change_warning(count-filtered_count, previous_count)

                self.log.info(f"{count-filtered_count} were dropped due to the suppresion rule ({reason})")

                count = filtered_count
            else:
                base_query += C1_APPEND_QUERY
                c1_count = spark.sql(base_query).collect()[0][0]

                base_query += query
                filtered_count = spark.sql(base_query).collect()[0][0]

                if previous_df[previous_df['Reason'] == reason].shape[0] != 0:
                    previous_count = previous_df[previous_df['Reason'] == reason]['Count'].item()
                    self.percentage_change_warning(c1_count-filtered_count, previous_count)

                suppression_df = suppression_df.append({'Reason': reason, 'Count': c1_count-filtered_count}, ignore_index=True)
                self.log.info(f"{c1_count-filtered_count} were dropped due to the suppresion rule ({reason})")
            
        other_suppressions = {'Opt-out list': opt_out_count,
                              'Compliance': compliance_count,
                              'Non marketable SIC for C2': non_marketable_count_c2,
                              'SIC2 exclusion': sic2_exclusion_count,
                              'State and zip code exclusion': state_zip_exclusion_count,
                              'Additional suppression': additional_suppression_count,
                              'PO box suppression': po_box_count,
                              'CCS Points not btwn 200-650': ccs_suppression_count,
                              'NCOA suppression': ncoa_suppression_count} 
        
        for key, value in other_suppressions.items():
            if previous_df[previous_df['Reason'] == key].shape[0] != 0:
                previous_count = previous_df[previous_df['Reason'] == key]['Count'].item()
                self.percentage_change_warning(value, previous_count)

            suppression_df = suppression_df.append({'Reason': key, 'Count': value}, ignore_index=True)

        self.log.info(f"{final_count} is the final count of the mailable population")
        suppression_df = suppression_df.append({'Reason': 'Final Mailable Population', 'Count': final_count}, ignore_index=True)

        suppression_df['run_date'] = self.run_dt.strftime('%Y-%m-%d')

        # Ensure 'Count' column is of consistent data type (DoubleType)
        suppression_df['Count'] = suppression_df['Count'].astype(float)

        self.load_pandas_df_to_databricks_db(df=suppression_df[['run_date', 'Reason', 'Count']]
                                                    , tab_name_in_databricks_db=f"suppression_count_history"
                                                    , write_mode='append', index=False, save_as_format='delta')
        
    def normalize_series(self, series, stop_words):
        """Normalizes a series of strings by removing stop words and punctuation and converting to lower case"""

        normalized = \
            series\
            .str.lower()\
            .str.replace('[^\w\s]','', regex=True)\
            .str.replace(r'\b'+r'\b|\b'.join(stop_words)+r'\b', '', regex=True)\
            .str.replace('\s+',' ', regex=True)\
            .str.strip()  

        return normalized
    
    def preprocess_business_names(self, business_names, use_stored_lemmatizations=True):
        """
        This function preprocesses business names for use in the C2 model that determines if a business is in a restricted segment.

        Parameters
        ----------
        business_names : pd.Series
            The business names to preprocess.
        use_stored_lemmatizations : bool
            If True, the function will use the stored lemmatizations in the BUSINESS_NAME_LEMMATIZATIONS_FILE.
            If False, it will re-lemmatize the business names which can take 3-4 hours.
        """        

        business_names = business_names.copy().set_flags(allows_duplicate_labels=True)

        self.log.info('Preprocessing business names: Removing numbers...')
        business_names = business_names.str.replace('\d+', 'num', regex=True)

        self.log.info('Preprocessing business names: Removing punctuation...')
        business_names = business_names.str.replace('[^\w\s]', '', regex=True)

        self.log.info('Preprocessing business names: Lowercasing...')
        business_names = business_names.str.lower()

        self.log.info('Preprocessing business names: Lemmatizing...')

        nltk.data.path.append("/databricks/conda/envs/databricks-ml/lib/python3.7/site-packages/nltk/corpus")
        nltk.data.path.append("/databricks/python3/lib/python3.8/site-packages/nltk/corpus")
        lemmatizer=nltk.stem.WordNetLemmatizer()

        # C2 records with <NA> Business Name will be lemmatized to "", should be considered as not marketable
        # Should add a Market Insight filter to not export C2 records with no business name
        lemmatize_document = lambda doc: '' if pd.isnull(doc) else ' '.join([lemmatizer.lemmatize(w) for w in nltk.word_tokenize(doc)])

        if use_stored_lemmatizations:
            self.log.info('Using stored lemmatizations...')

            try:
                name_mapper = pd.read_parquet(BUSINESS_NAME_LEMMATIZATIONS_FILE)['lemmatized_name']

                # pandas bug: "ValueError: StringArray requires a sequence of strings or pandas.NA"
                # seems to occur when names don't appear in the name mapper, which should just return NaN
                # convert to dict to avoid this error, even though it adds overhead
                name_mapper_dict = name_mapper.to_dict() 

                business_names_lemmed = business_names.map(name_mapper_dict, na_action='ignore')
                del name_mapper_dict
                
            except FileNotFoundError:
                self.log.info(f'Unable to find stored lemmatizations file {BUSINESS_NAME_LEMMATIZATIONS_FILE}. Will lemmatize all business names and store them.')
                business_names_lemmed = pd.Series(index=business_names.index, dtype=business_names.dtype)

        else:
            self.log.warning('NOT using stored lemmatizations. This will take a long time (~2-3 hours)...')
            business_names_lemmed = pd.Series(index=business_names.index, dtype=business_names.dtype)

        unlemmed_business_names = business_names_lemmed[business_names_lemmed.isna()]
        num_new_names_to_lemmatize = len(unlemmed_business_names)

        if num_new_names_to_lemmatize > 0:

            self.log.info(f'Found {num_new_names_to_lemmatize:,} new business names to lemmatize. Starting now...')

            for index in unlemmed_business_names.index:
                business_names_lemmed.at[index] = lemmatize_document(business_names.at[index])        

            self.log.info(f'Saving new business name lemmatizations to {BUSINESS_NAME_LEMMATIZATIONS_FILE}...')

            # save the old index
            old_index = business_names_lemmed.index.copy()
            business_names_lemmed.index = business_names
            name_mapper = pd.concat(
                [
                    business_names_lemmed.set_flags(allows_duplicate_labels=True),
                    name_mapper.set_flags(allows_duplicate_labels=True),
                    ],
                axis=0)
            name_mapper = name_mapper[~name_mapper.index.duplicated(keep='first')]
            name_mapper = name_mapper[~name_mapper.index.isna()]
            name_mapper.rename('lemmatized_name').to_frame().to_parquet(BUSINESS_NAME_LEMMATIZATIONS_FILE)

            business_names_lemmed.index = old_index

        self.log.info("Done preprocessing business names")

        return business_names_lemmed 
    
    def check_time_since_model_trained(self, model_file_path):
        """
        Checks the time since the model was last trained abd returns it in units of days.
        Assumes that the file modification time is the time the model was trained.
        Returns None if the model file does not exist.

        Parameters
        ----------
        model_file_path : str
            The path to the model file.
        """

        model_file_path = Path(model_file_path)

        if model_file_path.exists():
            last_modified = model_file_path.stat().st_mtime
            now = time.time()
            seconds_since_last_modified = now - last_modified
            days_since_last_modified = seconds_since_last_modified / (60 * 60 * 24)
            return days_since_last_modified
        else:
            return None
        
    def train_marketable_business_classifier(self, query_str, model_file_path):
        """
        This function trains the marketable business classifier. 
        Called automatically if days_since_last_trained > MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED 
        """

        log_topic = "Training marketable business classifier... | "
        self.log.info(log_topic)

        query = query_str.format(
            lyr = self.load_dt.year,
            lmon = self.load_dt.month,
            run_date = self.run_dt.strftime("%Y%m%d")
        )        
        df = spark.sql(query).toPandas()    
        df = df.drop_duplicates(subset=['duns'], keep='first')
        self.log.info(log_topic + f"Done reading in files with {df.shape[0]: 6,} rows")

        df['business_name_cleaned'] = self.preprocess_business_names(df['business_name'], use_stored_lemmatizations=True)

        tf_idf_transformer = TfidfVectorizer(ngram_range=(1, 2), min_df = 10)
        log_reg = LogisticRegression(C=0.5, solver="liblinear", multi_class="ovr")

        marketable_business_classifier = Pipeline([
            ('tf_idf_transformer', tf_idf_transformer),
            ('log_reg', log_reg),
        ])

        X_train, X_test, y_train, y_test = train_test_split(
            df['business_name_cleaned'],
            df['marketable'],
            test_size=0.1,
            random_state=42
            )

        marketable_business_classifier.fit(X_train, y_train)

        self.log.info(log_topic + 'Done training model')
        self.log.info(log_topic + 'Benchmarking model performance...')

        y_pred = marketable_business_classifier.predict(X_test)

        report = classification_report(
                            y_true = y_test,
                            y_pred = y_pred,
                            target_names= ['Restricted', 'Unrestricted'],
                            )

        self.log.info(log_topic + 'Classification Report:\n' + report)


        self.log.info(log_topic + 'Saving model...')
        
        with open(model_file_path, "wb") as f:
            pickle.dump(marketable_business_classifier, f, pickle.HIGHEST_PROTOCOL)

        self.log.info(log_topic + 'Done saving model')    
        
    def marketable_business_classification(self, business_name_cleaned, refit_model=False):
        """
        This function classifies business names as marketable or not marketable.

        Parameters
        ----------
        business_name_cleaned : pd.Series
            The preprocessed business names to classify. These should be lemmed and lowercased.
        refit_model : bool
            If True, the function will refit the model.
        """
        self.log.info("Reading in marketable business blassifier...")

        
        marketable_business_classifier_file_in_use = self.get_most_recent_marketable_business_model()
        days_since_last_trained = session.check_time_since_model_trained(model_file_path=marketable_business_classifier_file_in_use)

        config_log_message = f'(MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED = {MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED})'
        if refit_model or (days_since_last_trained > MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED):
            self.log.info(f'Marketable business classifier model {marketable_business_classifier_file_in_use} is {days_since_last_trained:,.0f} days old. It will be retrained. {config_log_message}')
            # reassign the model name with 1 version up
            marketable_business_model_name = marketable_business_classifier_file_in_use.split('_v')
            marketable_business_model_version = int(marketable_business_model_name[1][0])
            replacement = f'_v{marketable_business_model_version + 1}-0-0'
            marketable_business_classifier_file_in_use = re.sub(r"_v\d-0-0", replacement, marketable_business_classifier_file_in_use)
            self.log.info(f'training new model and renaming it as {marketable_business_classifier_file_in_use}')
            session.train_marketable_business_classifier(query_str=RETRAIN_C2_MARKETABLE_DATA_QUERY, model_file_path=marketable_business_classifier_file_in_use)
        else:
            self.log.info(f'Marketable business classifier model is {days_since_last_trained:,.0f} days old. It will be used. {config_log_message}')

        with open(marketable_business_classifier_file_in_use, "rb") as f:
            marketable_business_classifier = pickle.load(f)
            
        self.log.info("Done reading in marketable business classifier")
        
        
        self.log.info("Predicting class 2 marketability...")
        marketability_predictions = marketable_business_classifier.predict(business_name_cleaned)
        self.log.info("Done predicting class 2 marketability")

        return marketability_predictions


    def query_model_data(self, model_version):
        
        chg_vars_c1 =  ['chg_cosmetic_name_change_3m',
        'chg_location_ind_3m',
        'chg_operates_from_residence_ind_3m',
        'chg_owns_ind_3m',
        'chg_status_code_3m',
        'chg_avil_phy_addr_3m',
        'chg_rec_clas_typ_code_3m',
        'chg_small_business_ind_3m',
        'chg_buydex_n10pct_3m',
        'chg_mc_borrowing_growth_to_stable_3m']

        chg_vars_c2 =   ['chg_cosmetic_name_change_3m',
        'chg_location_ind_3m',
        'chg_operates_from_residence_ind_3m',
        'chg_owns_ind_3m',
        'chg_status_code_3m',
        'chg_avil_phy_addr_3m',
        'chg_rec_clas_typ_code_3m',
        'chg_small_business_ind_3m',
        'chg_composite_risk_3m', 
        'chg_triple_play_segment_3m']
        
        if model_version == 'V6':
            query_str = MODEL_DATA_QUERY_V6
        
            query = query_str.format(
            lyr = self.load_dt.year,
            lmon = self.load_dt.month,
            lyr_prior = self.load_dt_prior.year,
            lmon_prior = self.load_dt_prior.month,
            lyr_qtr = 2025,
            lmon_qtr = 6,
            dnb_score_table_name_c1 = self.get_most_recent_dnb_score_table_name(class_=1),
            dnb_score_table_name_c2 = self.get_most_recent_dnb_score_table_name(class_=2),
            db = self.db,
            run_date = self.run_dt.strftime('%Y%m%d')
            )                            
        
            df = spark.sql(query)

            df = df.withColumn('custom_funded', F.col("custom_funded").cast('int'))\
                .withColumn('custom_response', F.col("custom_response").cast('int'))\
                .withColumn('TIB', F.year(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd')) - df['bus_strt_yr'].cast(IntegerType()))\
                .withColumn('TIB', F.when(F.rtrim(F.ltrim(F.col('bus_strt_yr'))).isin(['0000','']), None).otherwise(F.col('TIB')))\
                .withColumn('ucc_filing_date', F.to_date(F.unix_timestamp(F.col('ucc_filing_date'), 'yyyyMMdd').cast("timestamp")))\
                .withColumn("months_since_ucc",F.round(F.months_between(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd'), F.col("ucc_filing_date")),2))\
                .withColumn("months_since_ucc",F.when(F.isnull(F.col("months_since_ucc")),F.lit(0)).otherwise(F.col("months_since_ucc")))\
                .withColumn('sic2', F.substring(F.col('sic4'), 1, 2))\
                .withColumn('qtr_mail', F.quarter(F.lit(self.mail_date_str)))\
                .withColumn('sic2_qtr', F.concat(F.col('sic2'), F.lit('_Q'), F.col('qtr_mail')))\
                .withColumn('chg_nbr', F.when(F.col('class_type')=='c1', reduce(add, [F.coalesce(F.col(x), F.lit(0)) for x in chg_vars_c1])).otherwise(reduce(add, [F.coalesce(F.col(x), F.lit(0)) for x in chg_vars_c2])))\
                .withColumn('chg_flg', F.when(F.col('chg_nbr')>0, F.lit(1)).otherwise(F.lit(0)))\
                .withColumn("months_since_ppp_approved",F.round(F.months_between(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd'), F.col("DateApproved")),2))\
                .withColumn("months_since_ppp_loan_status",F.round(F.months_between(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd'), F.col("LoanStatusDate")),2))\
                .withColumn("months_since_ppp_forgiveness",F.round(F.months_between(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd'), F.col("ForgivenessDate")),2))   

        elif model_version == 'V50':
            query_str = MODEL_DATA_QUERY_V50

            query = query_str.format(
                lyr = self.load_dt.year,
                lmon = self.load_dt.month,
                lyr_prior = self.load_dt_prior.year,
                lmon_prior = self.load_dt_prior.month,
                dnb_score_table_name_c1 = self.get_most_recent_dnb_score_table_name(class_=1),
                dnb_score_table_name_c2 = self.get_most_recent_dnb_score_table_name(class_=2),
                db = self.db
            )

            df = spark.sql(query)

            df = df.withColumn('custom_funded', F.col("custom_funded").cast('int'))\
                .withColumn('custom_response', F.col("custom_response").cast('int'))\
                .withColumn('TIB', F.year(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd')) - df['bus_strt_yr'].cast(IntegerType()))\
                .withColumn('TIB', F.when(F.rtrim(F.ltrim(F.col('bus_strt_yr'))).isin(['0000','']), None).otherwise(F.col('TIB')))\
                .withColumn('ucc_filing_date', F.to_date(F.unix_timestamp(F.col('ucc_filing_date'), 'yyyyMMdd').cast("timestamp")))\
                .withColumn("months_since_ucc",F.round(F.months_between(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd'), F.col("ucc_filing_date")),2))\
                .withColumn("months_since_ucc",F.when(F.isnull(F.col("months_since_ucc")),F.lit(0)).otherwise(F.col("months_since_ucc")))\
                .withColumn('sic2', F.substring(F.col('sic4'), 1, 2))\
                .withColumn('qtr_mail', F.quarter(F.lit(self.mail_date_str)))\
                .withColumn('sic2_qtr', F.concat(F.col('sic2'), F.lit('_Q'), F.col('qtr_mail')))\
                .withColumn('chg_nbr', F.when(F.col('class_type')=='c1', reduce(add, [F.coalesce(F.col(x), F.lit(0)) for x in chg_vars_c1])).otherwise(reduce(add, [F.coalesce(F.col(x), F.lit(0)) for x in chg_vars_c2])))\
                .withColumn('chg_flg', F.when(F.col('chg_nbr')>0, F.lit(1)).otherwise(F.lit(0)))    
        
        elif model_version == 'V7':
            query_str = MODEL_DATA_QUERY_V7
        
            query = query_str.format(
            lyr = self.load_dt.year,
            lmon = self.load_dt.month,
            lyr_prior = self.load_dt_prior.year,
            lmon_prior = self.load_dt_prior.month,
            lyr_qtr = self.lyr_qtr,
            lmon_qtr = self.lmon_qtr,
            db = self.db,
            run_date = self.run_dt.strftime('%Y%m%d')
            )                            
        
            df = spark.sql(query)
           
            df = df.withColumn('TIB', F.year(F.to_date(F.lit(self.mail_date_str), 'yyyy-MM-dd')) - df['bus_strt_yr'].cast(IntegerType()))\
                .withColumn('TIB', F.when(F.rtrim(F.ltrim(F.col('bus_strt_yr'))).isin(['0000','']), None).otherwise(F.col('TIB')))\
                .withColumn('qtr_mail', F.quarter(F.lit(self.mail_date_str)))\
                .withColumn('sic2_qtr', F.concat(F.col('sic2'), F.lit('_Q'), F.col('qtr_mail')))\
                .withColumn('chg_nbr', F.when(F.col('class_type')=='c1', reduce(add, [F.coalesce(F.col(x), F.lit(0)) for x in chg_vars_c1])).otherwise(reduce(add, [F.coalesce(F.col(x), F.lit(0)) for x in chg_vars_c2])))\
                .withColumn('chg_flg', F.when(F.col('chg_nbr')>0, F.lit(1)).otherwise(F.lit(0)))\
                

        return df
           
    def check_model_data(self):
        # important check!
        # need to have 10 unique values in the custom scores
        custom_score_check = spark.sql(CUSTOM_SCORE_CHECK_QUERY)
        dist_cnt_resp = custom_score_check.select(F.collect_list('distinct_cnt_resp')).first()[0][0]
        dist_cnt_funded = custom_score_check.select(F.collect_list('distinct_cnt_funded')).first()[0][0]
        assert dist_cnt_resp == 10, f"10 distinct values in col custom_response expected, got: {dist_cnt_resp}"
        assert dist_cnt_funded == 10, f"10 distinct values in col custom_funded expected, got: {dist_cnt_funded}"

        # check load_month of tabs
        tabs = [ 
        'us_marketing.dnb_core_id_geo_us'
        ,'us_marketing_features.dnb_us_marketing_basetable'
        ,'us_marketing.dnb_bus_dimensions_us'
        ,'us_marketing.dnb_core_bus_firm_hq_us'
        ,'us_marketing.dnb_core_legacy_payment_attrib_hq_us'
        ,'us_marketing.dnb_core_marketing_scores_us'
        ,'us_marketing.dnb_core_risk_scores_hq_us'
        ,'us_marketing.dnb_da_business_inquiries_hq_us'
        ,'us_marketing_features.dnb_id_geo_chg_us'
        ,'us_marketing_features.dnb_marketing_scores_chg_us'
        ,'us_marketing.dnb_layoff_score_us'
        ,'us_marketing.dnb_da_business_activity_hq_us'
        ,'us_marketing.dnb_da_location_site_us'
        ,'us_marketing_features.dnb_id_geo_chg_us'
        ,'us_marketing.dnb_da_dtri_hq_us'
        ,'us_marketing.dnb_core_legacy_payment_attrib_hq_us'
        ,'us_marketing.dnb_dsf_us'
  
        ]

        for tab in tabs: 
            sql = """
            SELECT max(load_month)
            FROM {dnb_tab}
            WHERE load_year = {lyr}
            """
            sql = sql.format(lyr = self.load_dt.year, dnb_tab = tab)   
            df = spark.sql(sql)
            self.log.info(f'TAB {tab} with latest load_month: {df.collect()[0][0]}')

    def get_num_times_mailed_in_time_period(self, cadence_pattern_series, period_end_num_months_ago, period_length_months):
        """
        This function uses the cadence pattern to determine how many times a business has been mailed in a given time period.
        We use this to create model features such as 
        - number of times mailed in the last 6 months -> period_end_num_months_ago = 0, period_length_months = 6
        - number of times mailed in the last 12 months -> period_end_num_months_ago = 0, period_length_months = 12
        - number of times mailed from 24 months ago to 12 months ago -> period_end_num_months_ago = 12, period_length_months = 12
        - number of times mailed from 36 months ago to 24 months ago -> period_end_num_months_ago = 24, period_length_months = 12

        Parameters
        ----------
        cadence_pattern_series : pd.Series of str
            The cadence pattern for each business. This is a string of 0s and 1s, where 1s indicate that the business has been mailed in that month.
            The first character in the string corresponds to the most recent month, and the last character corresponds to the oldest month.
            For example, if the string is "101", then the business has been mailed in the most recent month and 2 months ago.
            The length of the string should not be shorter than period_end_num_months_ago + period_length_months.

        period_end_num_months_ago : int
            The number of months ago that the time period ends. For example, if period_end_num_months_ago = 0, then the time period ends in the most recent month.
        period_length_months : int
            The length of the time period in months. For example, if period_length_months = 6, then the time period is 6 months long.
            This means we slice the cadence pattern string to get a length 6 substring that corresponds to the time period we are interested in.

        """

        if period_end_num_months_ago + period_length_months > len(cadence_pattern_series):
            raise ValueError("cadence pattern not long enough to cover time period")

        # index the cadence pattern string to only the relevant time period
        # e.g. first 12 months of 000010101011010100101 -> 000010101011 
        cad_pat_indexed_for_time_period = cadence_pattern_series.str[period_end_num_months_ago:period_end_num_months_ago+period_length_months]

        # cleverly split on '1' and count len of resulting list to get num times mailed in time period
        # e.g. 000010101011 -> ['0000', '0', '0', '0', ''] -> 5
        num_times_mailed_in_time_period = cad_pat_indexed_for_time_period.str.split('1').apply(len) - 1

        return num_times_mailed_in_time_period.astype(int)

    def conduct_brand_specific_feature_engineering(self, model_data):
        """
        conduct_brand_specific_feature_engineering
        """
        self.log.info('Starting to conduct brand specific feature engineering')
        
        # only mailable businesses
        df_brand = model_data.withColumn("months_since_first", \
                                F.round(F.months_between(F.to_date(F.lit(session.mail_date_str), 'yyyy-MM-dd'), F.col(f"{brand}_first_mail_date")), 2))\
                            .withColumn("months_since_last", \
                                F.round(F.months_between(F.to_date(F.lit(session.mail_date_str), 'yyyy-MM-dd'), F.col(f"{brand}_last_mail_date")), 2))
        pd_brand = df_brand.toPandas()

        # # months between first mail date, last mail date
        pd_brand['months_since_first'] = np.where(pd_brand['months_since_first'] > 0, pd_brand['months_since_first'], 0)
        pd_brand['months_since_last'] = np.where(pd_brand['months_since_last'] > 0, pd_brand['months_since_last'], 0)

        for i in range(0,6):
            pd_brand[f'cadence_m{i+1}'] = np.where(pd_brand[f'{brand}_cadence_pattern'].str[i]=='1', 1, 0)

        pd_brand['num_times_mailed'] = np.where(pd_brand[f'{brand}_num_times_mailed'] > 0, pd_brand[f'{brand}_num_times_mailed'], 0)
        pd_brand['flg_mailed'] = np.where(pd_brand['num_times_mailed'] > 0, 1, 0)

        
        pd_brand[f'{brand}_cadence_pattern'].fillna('0'*36, inplace=True)
        for num_months in [6, 12, 24, 36]:
            pd_brand[f'mail_{num_months}mo'] = session.get_num_times_mailed_in_time_period(
                    cadence_pattern_series = pd_brand[f'{brand}_cadence_pattern'],
                    period_end_num_months_ago = 0, 
                    period_length_months = num_months,
                    )

        pd_brand['mailed_over_12mo'] = 0
        pd_brand.loc[(pd_brand['num_times_mailed'] > 1) & (pd_brand['num_times_mailed'] > pd_brand['mail_12mo']) , 'mailed_over_12mo'] = 1
        pd_brand.rename(columns = {f'{brand}_cadence_pattern':'cadence_pattern'
                                    , f'{brand}_flg_branddup_prior':'flg_branddup_prior'}, inplace = True)
        pd_brand.drop(columns = [c for c in pd_brand.columns if (c[:3] in ('nf_', 'qb_')) & (c not in ['nf_mailable', 'qb_mailable', 'nf_num_times_mailed', 'qb_num_times_mailed'])], inplace=True)

        # change variables
        for chg_col in pd_brand.columns:
            if chg_col[:4] == 'chg_':
                pd_brand[chg_col].fillna(0, inplace=True)

        return pd_brand
    
    def before_loading_data_to_table(self, table_name, mo_before = None):
        """
        keep table with rolling mo_before months period
        """
        run_dt_current = self.first_of_this_month.strftime('%Y-%m-%d')
        if mo_before:
            run_dt_months_prior = (self.first_of_this_month + relativedelta(months=-mo_before)).strftime('%Y-%m-%d')
            spark.sql(f"""DELETE FROM nf_workarea.{table_name}
                        WHERE (run_date <= '{run_dt_months_prior}') OR (run_date = '{run_dt_current}')""")
            self.log.info(f'Done deleting old historical scores from table {table_name} before and including {run_dt_months_prior}')
        else:
            spark.sql(f"""DELETE FROM nf_workarea.{table_name}
                        WHERE (run_date = '{run_dt_current}')""")
            self.log.info(f'Done deleting historical scores from table {table_name} from current month')

    def generate_duns_ctf_projections(self, model_version):

        projections = spark.sql(DUNS_CTF_PROJECTION_QUERY.format(db = session.db, version = model_version, tib_suppression_month = self.tib_suppression_date.month, tib_suppression_year = self.tib_suppression_date.year))
        projections = projections.withColumn('ctf_nf', F.when((projections.sic2.isin(['42', '47'])) & (projections.ppp == 'N') & (projections.ctf_nf != 99999), projections.nf_CPP/(projections.nf_rate_resp*projections.nf_rate_sub*0.2*projections.nf_rate_fund*projections.nf_fund_amt_per_deal)).otherwise(projections.ctf_nf))\
                                .withColumn('ctf_qb', F.when((projections.sic2.isin(['42', '47'])) & (projections.ppp == 'N') & (projections.ctf_qb != 99999), projections.qb_CPP/(projections.qb_rate_resp*projections.qb_rate_sub*0.2*projections.qb_rate_fund*projections.qb_fund_amt_per_deal)).otherwise(projections.ctf_qb))
        projections.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f'{self.db}.duns_ctf_projections_{model_version}')

    def conduct_final_selection_main(self, bau_nf_volume, bau_qb_volume, query_with_constraint, sbl_query, final_changes_query, model_version, sbl_nf_only_count = SBL_NF_ONLY_COUNT, sbl_qb_only_count = SBL_QB_ONLY_COUNT, 
    nf_dbl_select_volume = 0, qb_dbl_select_volume = 0, confirmed_scenario = False):

        selection_target_volume_nf = bau_nf_volume + sbl_nf_only_count - nf_dbl_select_volume
        selection_target_volume_qb = bau_qb_volume + sbl_qb_only_count
        unique_name_volume = selection_target_volume_nf + selection_target_volume_qb 
        mail_volume = bau_nf_volume + bau_qb_volume

        print("Unique Business Count: ", unique_name_volume)
        print("Total Mail Count: ", mail_volume)

        # conduct final selection and create the final selection tab
        updated_query = query_with_constraint.format(db = self.db
                                    , nf_unique_vol=selection_target_volume_nf
                                    , qb_unique_vol=selection_target_volume_qb
                                    , nf_dbl_mail_volume=nf_dbl_select_volume
                                    , qb_dbl_mail_volume=qb_dbl_select_volume
                                    , version = model_version
                                    , run_date = self.run_dt.strftime('%Y%m%d')
                                    , prescreen_campaign_month = self.mail_dt.strftime('%Y%m'))
            
        df = spark.sql(updated_query)
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f'{self.db}.final_selection_original_{model_version}')

        query = sbl_query.format(db = self.db,
                                            SBL_NF_ONLY_COUNT = sbl_nf_only_count,
                                            SBL_QB_ONLY_COUNT = sbl_qb_only_count,
                                            run_date = self.run_dt.strftime("%Y%m%d"),
                                            version = model_version
                                            )
        df = spark.sql(query)
        df.write.mode("overwrite").format("parquet").saveAsTable(f'{self.db}.sbl_selection')
        self.log.info(f"Done SBL selection with {df.count(): 6,} rows")

        final_selection_changes_query = final_changes_query.format(db = self.db, version = model_version, run_date = self.run_dt.strftime("%Y%m%d"))

        df = spark.sql(final_selection_changes_query)
        df.write.mode("overwrite").format("parquet").saveAsTable(f'{self.db}.final_selection_updated_{model_version}')
        self.log.info(f"Finished updating final selection table")
        
        if confirmed_scenario:
            df = df.withColumn('run_date', F.lit(self.run_dt.strftime('%Y-%m-%d'))) \
                .select('duns', 'nf_selected', 'qb_selected', 'nf_rate_resp', 'nf_rate_sub',
                        'nf_rate_appr', 'nf_rate_fund', 'nf_fund_amt_per_deal', 'nf_CPP',
                        'ctf_nf', 'qb_rate_resp', 'qb_rate_sub', 'qb_rate_appr', 'qb_rate_fund',
                        'qb_fund_amt_per_deal', 'qb_CPP', 'ctf_qb', 'run_date', 'nf_rate_qual',
                        'qb_rate_qual')
            if self.running_in_prod:
                self.before_loading_data_to_table('final_selection_history', mo_before = None)
            df.write.mode("append").format("delta").saveAsTable(f'{self.db}.final_selection_history')
        
    def generate_ready_to_be_licensed(self, tables, main_version):

        e_rdd = spark.sparkContext.emptyRDD()
        e_sch = spark.sql(f"SELECT * FROM {tables[0]}").schema
        df = spark.createDataFrame(data=e_rdd, schema=e_sch)

        for version in tables:
            df_version = spark.sql(f"SELECT * FROM {version}")
            df = df.union(df_version)
        df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f'{self.db}.final_selection_updated_complete')
        
        query = READY_TO_BE_LICENSED_QUERY.format(db = self.db,
                                                  lyr = self.load_dt.year,
                                                  lm = self.load_dt.month,
                                                  lyr_prior = self.load_dt_prior.year,
                                                  lm_prior = self.load_dt_prior.month,
                                                  main_version = main_version)

        df = spark.sql(query)
        df.write.mode("overwrite").format("parquet").option("mergeSchema", "true").saveAsTable(f'{self.db}.ready_to_be_licensed')
        self.log.info(f"Done creating tab ready_to_be_licensed with {df.count(): 6,} rows")
    
    def query_ctf_calib_data(self, start_dt, end_dt, model_version):
        """
            This function collect data for CTF calibration table.
        """
        query = CTF_CALIB_TAB_QUERY.format(calib_start_dt = start_dt, calib_end_dt=end_dt, db = self.db, model_version = model_version)
        df = spark.sql(query)

        cols = ['duns', 'mail_date', 'campaignid', 'accountnum', 'class_type',
                'proba_RESP', 'mdl_name_RESP', 'proba_SUB', 'mdl_name_SUB', 'ppp']
        
        for brand in ['nf', 'qb']:
            brand_df = df.filter(f"{brand}_mailable = True")\
                    .withColumnRenamed(f"{brand}_campaignid", "campaignid")\
                    .withColumnRenamed(f"{brand}_accountnum", "accountnum")\
                    .withColumnRenamed(f"{brand}_proba_RESP", "proba_RESP")\
                    .withColumnRenamed(f"{brand}_mdl_name_RESP", "mdl_name_RESP")\
                    .withColumnRenamed(f"{brand}_proba_SUB", "proba_SUB")\
                    .withColumnRenamed(f"{brand}_mdl_name_SUB", "mdl_name_SUB")\
                    .select(*cols)
                    
            brand_df.write.mode("overwrite").format("parquet").saveAsTable(f'{self.db}.ctf_calib_data_{brand}_{model_version}')
            
    def generate_calibration_table(self, input_df, measure, model_version = None):
        ################################################
        # assign variables
        ################################################
        if measure == 'resp':
            denominator = 'duns'
            numerator = 'flg_resp'
            grouper = ['resp_segment']
            if model_version == 'V6':
                rolling_window = ['ppp','brand','class_type', 'scorebin_SUB_grp']
            else:
                rolling_window = ['brand','class_type', 'scorebin_SUB_grp']
            metric_list = ['duns', 'flg_resp', 'rate_resp', 'rate_qual']
            minimum_sample_size = 5000
        elif measure == 'sub':
            denominator = 'flg_resp'
            numerator = 'flg_sub'
            if model_version == 'V6':
                grouper = ['sub_segment']
            else:
                grouper = ['brand', 'class_type', 'scorebin_SUB']
            rolling_window = ['ppp','brand','class_type']
            metric_list = ['flg_resp', 'flg_sub', 'rate_sub']
            minimum_sample_size = 50
        elif measure == 'appr':
            denominator = 'flg_sub'
            numerator = 'flg_appr'
            grouper = ['appr_segment']
            metric_list = ['flg_sub', 'flg_appr', 'rate_appr', 'rate_appr_obs']
            minimum_sample_size = 10
        elif measure == 'fund':
            denominator = 'flg_appr'
            numerator = 'flg_fund'
            grouper = ['fund_segment']
            metric_list = ['flg_appr', 'flg_fund', 'rate_fund', 'fund_amt_per_deal', 'rate_fund_obs', 'fund_amt_obs_per_deal']
            minimum_sample_size = 0

        ################################################
        # prepare base dataframe and aggregated summary
        ################################################
        base_df = ctf_base[measure][grouper + metric_list]
        aggr = input_df.groupby(grouper).agg({'duns':'count', 'flg_resp':'sum', 'flg_qual':'sum', 'flg_sub':'sum', 'flg_appr':'sum', 'flg_appr_obs': 'sum', 'flg_fund':'sum', 'fund_amt':'sum', 'flg_fund_obs':'sum', 'fund_amt_obs':'sum'})

        ################################################
        # generate raw rate and apply smoothing factor
        ################################################
        aggr['raw_rate'] = (aggr[numerator] / aggr[denominator]).astype(float)
        aggr[f'rate_{measure}_orig'] = (aggr[numerator] / (aggr[denominator])).fillna(0).astype(float)

        if measure == 'resp':
            aggr['rate_qual'] = (aggr['flg_qual'] /  aggr['duns']).fillna(0).astype(float)

        if measure == 'appr':
            aggr[f'rate_{measure}_obs'] = (aggr[numerator+'_obs'] / (aggr[denominator])).fillna(0).astype(float)
        elif measure == 'fund':
            aggr[f'rate_{measure}_obs'] = (aggr[numerator+'_obs'] / (aggr[denominator+'_obs'])).fillna(0).astype(float)

            # use mean to estimate funded amt per deal
            fund_amt_per_deal = input_df[input_df['flg_fund']==1].groupby(grouper)['fund_amt'].mean().round(0)
            aggr = aggr.merge(fund_amt_per_deal, how='left', on=grouper, suffixes=['', '_per_deal_orig'])

            # use mean to estimate funded amt per deal
            fund_amt_per_deal_obs = input_df[input_df['flg_fund_obs']==1].groupby(grouper)['fund_amt_obs'].mean().round(0)
            aggr = aggr.merge(fund_amt_per_deal_obs, how='left', on=grouper, suffixes=['', '_per_deal_orig'])

        ################################################
        # merge with base table
        ################################################
        # change data type from categorical to str
        aggr = aggr.reset_index()
        cat_data = aggr.select_dtypes(include='category')
        for c in cat_data.columns:
            aggr[c] = aggr[c].astype('str')

        # merge base table
        dict_rename_metric = {metric_list[i]: metric_list[i]+'_base' for i in range(len(metric_list))}
        base_df.rename(columns=dict_rename_metric, inplace=True)

        aggr = aggr.merge(base_df, how='left', left_on=grouper, right_on=grouper, validate='one_to_one')

        ################################################
        # decide to use latest rates or baseline rates
        ################################################
        # if count is greater than minimum sample size or count in baseline data, 
        # use the latest stat, otherwise use baseline 
        aggr[f'rate_{measure}'] = np.where(((aggr[denominator] >= minimum_sample_size) | (aggr[denominator] >= aggr[f'{denominator}_base']))
                                                        , aggr[f'rate_{measure}_orig'], aggr[f'rate_{measure}_base'])
        if measure == 'appr':
            aggr[f'rate_{measure}_obs'] = (aggr[numerator+'_obs'] / (aggr[denominator])).fillna(0).astype(float)
        elif measure == 'fund':
            aggr[f'rate_{measure}_obs'] = (aggr[numerator+'_obs'] / (aggr[denominator+'_obs'])).fillna(0).astype(float)

            aggr['fund_amt_per_deal'] = np.where(((aggr['flg_appr'] >= minimum_sample_size) | (aggr['flg_appr'] >= aggr['flg_appr_base']))
                                                        , aggr['fund_amt_per_deal_orig'], aggr['fund_amt_per_deal_base'])
            
            aggr['fund_amt_obs_per_deal'] = np.where(((aggr['flg_appr'] >= minimum_sample_size) | (aggr['flg_appr'] >= aggr['flg_appr_base']))
                                                        , aggr['fund_amt_obs_per_deal_orig'], aggr['fund_amt_obs_per_deal_base'])

        ################################################
        # apply moving avg
        ################################################
        if measure == 'resp':
            if version == 'V6':
                mapping = pd.DataFrame(list(CTF_RESP_SEG[version].values()), index = pd.MultiIndex.from_tuples(CTF_RESP_SEG[version].keys())).reset_index().rename(columns = {0: 'resp_segment',
                                                                                                                                                        'level_0': 'ppp',
                                                                                                                                                        'level_1': 'brand',
                                                                                                                                                        'level_2': 'class_type',
                                                                                                                                                        'level_3': 'scorebin_SUB_grp',
                                                                                                                                                        'level_4': 'scorebin_RESP'})
            else:
                mapping = pd.DataFrame(list(CTF_RESP_SEG[version].values()), index = pd.MultiIndex.from_tuples(CTF_RESP_SEG[version].keys())).reset_index().rename(columns = {0: 'resp_segment',
                                                                                                                                                            'level_0': 'brand',
                                                                                                                                                            'level_1': 'class_type',
                                                                                                                                                            'level_2': 'scorebin_SUB_grp',
                                                                                                                                                            'level_3': 'scorebin_RESP'})
            aggr = mapping.merge(aggr)

        elif (measure == 'sub') & (model_version == 'V6'):
            mapping = pd.DataFrame(list(CTF_SUB_SEG.values()), index = pd.MultiIndex.from_tuples(CTF_SUB_SEG.keys())).reset_index().rename(columns = {0: 'sub_segment',
                                                                                                                                                    'level_0': 'ppp',
                                                                                                                                                    'level_1': 'brand',
                                                                                                                                                    'level_2': 'class_type',
                                                                                                                                                    'level_3': 'scorebin_SUB'})
            aggr = mapping.merge(aggr)

        if measure == 'resp':
            aggr[f'rate_{measure}'] = aggr.groupby(rolling_window)[f'rate_{measure}'].transform(lambda x: x.rolling(2, 1).mean())

        for col in ['flg_resp', 'flg_qual', 'flg_sub', 'flg_appr', 'flg_fund', 'flg_appr_obs', 'flg_fund_obs']:
            aggr[col] = aggr[col].astype(float)

        ################################################
        # make two versions of outputs: detailed and simplified
        ################################################
        if measure == 'resp':
            aggr_simplified = aggr[rolling_window + ['scorebin_RESP'] + grouper + [c for c in metric_list if (c != 'duns') & ('flg_' not in c)]]
        elif (measure == 'sub') & (version == 'V6'):
            aggr_simplified = aggr[rolling_window + ['scorebin_SUB'] + grouper + [c for c in metric_list if (c != 'duns') & ('flg_' not in c)]]
        else:
            aggr_simplified = aggr[grouper + [c for c in metric_list if (c != 'duns') & ('flg_' not in c)]]

        return aggr, aggr_simplified

    def before_loading_data_to_calib_details(self, measure, version, tab_name):
        """
        clean up duplicates
        """
        spark.sql(f"""DELETE FROM {self.db}.{tab_name}
                    WHERE run_date = '{self.first_of_this_month.strftime("%Y-%m-%d")}'""")
        self.log.info(f'Done deleting duplicates in Tab {self.db}.{tab_name}')

    def generate_model_variable_distribution(self, model_version):
        col_bins = {
            'layoff_score_state_percentile': [-np.inf, 5, 7, 9, 11, 14, 17, 24, 33, 41, np.inf],
            'totdoll_hq': [-np.inf, 50, 100, 150, 500, 1300, 5000, 15300, np.inf],
            'npayexp': [-np.inf, 1.0, 2.0, 3.0, 5.0, np.inf],
            'payexp_s': [-np.inf, 1.0, 2.0, 3.0, np.inf],
            'inq_inquiry_duns_12m': [-np.inf, 0.0, 2.0, 3.0, 6.0, np.inf],
            'regular_inq_cnt': [-np.inf, 1.0, 2.0, np.inf],
            'bri_seg': [-np.inf, 6.0, np.inf],
            'TIB': [-np.inf, 1.0, 2.0, 3.0, 4.0, 5.0, 7.0, 10.0, 13.0, 21.0, np.inf],
            'months_since_ucc': [-np.inf, 0.0, np.inf],
            'nf_months_since_first': [-np.inf, 7.0, 13.0, 18.0, 28.0, 44.0, 53.0, 66.0, 78.0, 89.0, np.inf],
            'nf_months_since_last': [-np.inf, 2.0, 6.0, 9.0, 13.0, 19.0, 31.0, 47.0, 59.0, 73.0, np.inf],
            'qb_months_since_first': [-np.inf, 7.0, 11.0, 16.0, 22.0, 30.0, 36.0, 50.0, 60.0, 70.0, np.inf],
            'qb_months_since_last': [-np.inf, 2.0, 6.0, 9.0, 13.0, 18.0, 26.0, 36.0, 52.0, 62.0, np.inf],
            'cadence_m0': [-np.inf, 0.0, np.inf],
            'cadence_m1': [-np.inf, 0.0, np.inf],
            'cadence_m2': [-np.inf, 0.0, np.inf],
            'cadence_m3': [-np.inf, 0.0, np.inf],
            'cadence_m4': [-np.inf, 0.0, np.inf],
            'cadence_m5': [-np.inf, 0.0, np.inf],
            'mail_6mo': [-np.inf, 0.0, 1.0, np.inf],
            'mail_12mo': [-np.inf, 0.0, 1.0, np.inf],
            'mail_24mo': [-np.inf, 0.0, 1.0, 2.0, np.inf],
            'mail_36mo': [-np.inf, 0.0, 1.0, 3.0, np.inf],
            'gc_sales': [-np.inf, 51529, 61935, 64345, 73140, 85053, 109356, 168097, 265197, 326534,np.inf],
            'TIB': [-np.inf,1,6,np.inf],
            'gc_employees': [-np.inf,1, 2, 3, np.inf],
            'nf_num_times_mailed': [-np.inf, 1, 2, 3, 4, 5, 8, np.inf],
            'qb_num_times_mailed': [-np.inf, 1, 2, 3, 5, np.inf],
            'inq_inquiry_duns_6m': [-np.inf, 0, 2, 4, np.inf],
            'inq_sic61_inq_12m': [-np.inf, 0, np.inf],
            'regular_inq_cnt': [-np.inf, 1, 2, np.inf],
            'ccs_points': [-np.inf, 459., 469., 472., 474., 481., 487., 495., 515, 561, np.inf],
            'd_sat_hq': [-np.inf, 0, 100, 875, 3500, 13500, np.inf],
            'ba_cnt_cr_3m': [-np.inf, 0, 1, 2, 3, 6, np.inf],
            'inq_inquiry_duns_24m': [-np.inf, 0, 2, 4, 6, 12, np.inf],
            'totdollhq': [-np.inf, 0, 50, 100, 150, 500, 1300, 5050, 15600, np.inf],
            'nbr_ucc_filings': [-np.inf, 0, 1, np.inf],
            'ba_cnt_cr_12m': [-np.inf, 1, 2, 3, 4, 7, 12, 23, np.inf],
            'buydex': [-np.inf, 0, 5, 9, 13, 18, 24, 41, 55, 66, 78, np.inf],
            'tlp_score': [-np.inf, 2002, 2463, 2480, 2496, 2508, 2521, 2528, 2567, 2617, 2649, np.inf],
            'dt_12m_hi_cr_mo_av_amt': [-np.inf, 0, 21, 110, 236, 429, 759, 1427, 2879, 6023, 14430, np.inf],
            'less_than_truckload_buydex': [-np.inf, 0, 8, 18, 44, 69, 77, 83, 88, 92, 96, np.inf],
            'advertising_buydex': [-np.inf, 0, 5, 10, 14, 19, 23, 35, 49, 64, 79, np.inf],
            'months_since_ucc': [-np.inf, 0.0, np.inf],
            'chg_nbr': [-np.inf, 0, 1, 2, np.inf],
            'pydx_19': [-np.inf, 59.0, 70.0, 77.0, 80.0, 80.0, np.inf],
            'dolcur': [-np.inf, 25.00, 3600.00, np.inf],
            'ba_sum_f_mtch8_12m': [-np.inf, 3.00, 6.00, 13.00, np.inf],
            'drp_ccs9_prctl_prf_index': [-np.inf, 0.4857142857142857, 0.7083333333333334, 0.8372093023255814, 0.9523809523809523, 1.0, 1.0, 1.2142857142857142, 1.6, 2.380952380952381, np.inf],
            'months_since_ppp_approved': [-np.inf, 49.16, 50.48, 51.52, 59.45, 60.0, 60.06, 60.13, 60.55, 60.71, np.inf],
            'drp_fspct7p1_prf_index': [-np.inf, 0.42857142857142855, 0.6086956521739131, 0.7534246575342466, 0.9166666666666666, 1.0, 1.08, 1.2244897959183674, 1.48, 1.9189189189189189, np.inf],
            'drp_ccs9_prctl_loc_index': [-np.inf, 0.5, 0.7241379310344828, 0.8333333333333334, 0.9285714285714286, 1.0, 1.0144927536231885, 1.28, 1.837837837837838, 2.64, np.inf],
            'months_since_ppp_forgiveness': [-np.inf, 40.29, 42.65, 44.06, 45.32, 46.58, 48.13, 49.29, 50.52, 51.81, np.inf]
        }
                    
        model_input = spark.sql(f"""SELECT * FROM {self.db}.model_data_input_{model_version}""")
        df = model_input.withColumn("nf_months_since_first", \
                    F.round(F.months_between(F.to_date(F.lit(session.mail_date_str), 'yyyy-MM-dd'), F.col(f"nf_first_mail_date")), 2))\
                .withColumn("nf_months_since_last", \
                    F.round(F.months_between(F.to_date(F.lit(session.mail_date_str), 'yyyy-MM-dd'), F.col(f"nf_last_mail_date")), 2))\
                .withColumn("qb_months_since_first", \
                    F.round(F.months_between(F.to_date(F.lit(session.mail_date_str), 'yyyy-MM-dd'), F.col(f"qb_first_mail_date")), 2))\
                .withColumn("qb_months_since_last", \
                    F.round(F.months_between(F.to_date(F.lit(session.mail_date_str), 'yyyy-MM-dd'), F.col(f"qb_last_mail_date")), 2))
                
        ppp_cols = spark.sql("""select * from nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed LIMIT 5""").toPandas().columns
        col_list = [c for c in df.columns if ('date' not in c.lower())& (c not in ['duns', 'nf_mailable', 'qb_mailable']) & (c not in ppp_cols)]

        schema = StructType([
            StructField("value", StringType(), True),
            StructField("count", LongType(), True),
            StructField("variable", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("pcnt", DoubleType(), True),
            StructField("run_date", StringType(), True)
            ])
        variable_dist = sqlContext.createDataFrame(sc.emptyRDD(), schema)

        for brand in ['nf', 'qb']:
            for column_name in col_list:
                brand_df = df.filter(F.col(f'{brand}_mailable') == True)

                if column_name not in col_bins.keys():
                    dist = brand_df.groupBy(column_name).count()
                    dist = dist.withColumn('variable', F.lit(column_name))\
                                .withColumn('brand', F.lit(brand))\
                                .withColumn('pcnt', F.col('count')/F.sum('count').over(Window.partitionBy()))\
                                .withColumn('run_date', F.lit(self.run_dt.strftime("%Y-%m-%d")))\
                                .withColumnRenamed(column_name, 'value')
                else:
                    labels = {}
                    for i in range(len(col_bins[column_name])-1):
                        labels[i+1] = f'({col_bins[column_name][i]}, {col_bins[column_name][i+1]}]'

                    splits = list(enumerate(col_bins[column_name]))
                    bins = reduce(lambda c, i: c.when(F.col(column_name) <= i[1], i[0]), splits, F.when(F.col(column_name) < splits[0][0], None)).alias('bins')
                    df_bins = brand_df.select(bins)

                    mapping = F.create_map([F.lit(x) for x in chain(*labels.items())])
                    df_bins = df_bins.select(mapping[df_bins["bins"]].alias(f"{column_name}_bins"))
                    dist = df_bins.groupBy(f"{column_name}_bins").count()
                    dist = dist.withColumn('variable', F.lit(column_name))\
                                .withColumn('brand', F.lit(brand))\
                                .withColumn('pcnt', F.col('count')/F.sum('count').over(Window.partitionBy()))\
                                .withColumn('run_date', F.lit(self.run_dt.strftime("%Y-%m-%d")))\
                                .withColumnRenamed(column_name, 'value')

                variable_dist = variable_dist.union(dist)

        self.before_loading_data_to_table(f"model_variable_distribution_{model_version}")
        variable_dist.write.mode("append").format("delta").saveAsTable(f'nf_workarea.model_variable_distribution_{model_version}')
        self.log.info(f"Done including in data from {self.first_of_this_month.strftime('%Y-%m-%d')} to nf_workarea.model_variable_distribution_{model_version}")

    def model_data_distribution_comparison(self, date_of_interest, baseline = True, date_of_comparison = None, threshold = 0.1):

        try:
            spark.sql("DROP TABLE nf_workarea.AMP_Monthly_Reporting_PSI")
        except:
            pass

        if baseline:
            date_of_comparison = '2025-03-01'
        elif baseline == False & date_of_comparison is None:
            self.log_and_raise(ValueError(f"CANNONT CONTINUE ON UNTIL A COMPARISON DATE IS SPECIFIED."))

        current_dist = spark.sql(f"select * from nf_workarea.model_variable_distribution_v6 where run_date = '{date_of_interest}'").toPandas()
        base_dist = spark.sql(f"select * from nf_workarea.model_variable_distribution_v6 where run_date = '{date_of_comparison}'").toPandas()
        merged_dist_raw = current_dist.merge(base_dist, how='outer', on=['brand', 'variable', 'value'], suffixes=['_curr', '_base'])

        merged_dist_mail = pd.DataFrame()

        for brand in ['nf','qb']:
            
            for i in range(0,6):
                pd_brand = merged_dist_raw[(merged_dist_raw['variable']==f'{brand}_cadence_pattern')&(merged_dist_raw['brand']==brand)]
                pd_brand['value'] = np.where(pd_brand['value'].str[i]=='1', 1, 0)
                pd_brand['variable'] = f'cadence_m{i}'
                pd_brand_curr = pd_brand[pd_brand['run_date_curr'].isna()==False].groupby(['variable','brand','value', 'run_date_curr'], dropna=False).agg({'count_curr':'sum', 'pcnt_curr':'sum'}).reset_index()
                pd_brand_base = pd_brand[pd_brand['run_date_base'].isna()==False].groupby(['variable','brand','value', 'run_date_base'], dropna=False).agg({'count_base':'sum', 'pcnt_base':'sum'}).reset_index()
                pd_brand = pd_brand_curr.merge(pd_brand_base, how='outer', on=['variable','brand','value'])
                merged_dist_mail = pd.concat([merged_dist_mail, pd_brand])
            
            # mail_mo
            for num_months in [6, 12, 24, 36]:
                pd_brand = merged_dist_raw[(merged_dist_raw['variable']==f'{brand}_cadence_pattern')&(merged_dist_raw['brand']==brand)]
                pd_brand['value'] = pd_brand['value'].fillna('0'*36)
                pd_brand['value'] = session.get_num_times_mailed_in_time_period(
                    cadence_pattern_series = pd_brand['value'],
                    period_end_num_months_ago = 0, 
                    period_length_months = num_months,
                    )
                pd_brand['variable'] = f'mail_{num_months}mo'
                pd_brand_curr = pd_brand[pd_brand['run_date_curr'].isna()==False].groupby(['variable','brand','value', 'run_date_curr'], dropna=False).agg({'count_curr':'sum', 'pcnt_curr':'sum'}).reset_index()
                pd_brand_base = pd_brand[pd_brand['run_date_base'].isna()==False].groupby(['variable','brand','value', 'run_date_base'], dropna=False).agg({'count_base':'sum', 'pcnt_base':'sum'}).reset_index()
                pd_brand = pd_brand_curr.merge(pd_brand_base, how='outer', on=['variable','brand','value'])
                merged_dist_mail = pd.concat([merged_dist_mail, pd_brand])
            
            # sic1
            pd_brand = merged_dist_raw[(merged_dist_raw['variable']=='sic4')&(merged_dist_raw['brand']==brand)]
            pd_brand['value'] = pd_brand['value'].str.slice(0,1)
            pd_brand['variable'] = 'sic1'
            pd_brand_curr = pd_brand[pd_brand['run_date_curr'].isna()==False].groupby(['variable','brand','value', 'run_date_curr'], dropna=False).agg({'count_curr':'sum', 'pcnt_curr':'sum'}).reset_index()
            pd_brand_base = pd_brand[pd_brand['run_date_base'].isna()==False].groupby(['variable','brand','value', 'run_date_base'], dropna=False).agg({'count_base':'sum', 'pcnt_base':'sum'}).reset_index()
            pd_brand = pd_brand_curr.merge(pd_brand_base, how='outer', on=['variable','brand','value'])
            merged_dist_mail = pd.concat([merged_dist_mail, pd_brand])

        merged_dist = pd.concat([merged_dist_raw, merged_dist_mail])
        merged_dist['value_bin'] = merged_dist['value']

        merged_dist_binned = merged_dist.groupby(['brand', 'variable', 'value_bin'], dropna=False)['count_curr', 'pcnt_curr', 'count_base', 'pcnt_base'].sum().reset_index()
        
        # save var psi & save the var psi tables
        var_psi = {'nf':dict(), 'qb':dict()}
        tab_psi = pd.DataFrame()

        exclude_variables = ['nf_cadence_pattern','qb_cadence_pattern', 'sic4', 'bus_strt_yr', 'sic2_qtr', 'borrowing_capacity_index', 'qtr_mail', 'lineofcredit_propensity_segment', 'lease_propensity_segment', 'loan_propensity_segment', 'total_balance_segment', 'comptype_hq', 'pydx_1', 'lease_balance_segment']
        for brand in ['nf', 'qb']:
            for key in merged_dist['variable'].unique():
                if (key not in exclude_variables):
                    df_proportions = merged_dist_binned[(merged_dist_binned['variable']==key)&(merged_dist_binned['brand']==brand)]
                    comparison_proportions = df_proportions['pcnt_curr']
                    baseline_proportions = df_proportions['pcnt_base']
                    comparison_proportions[comparison_proportions == 0] = 0.000001
                    baseline_proportions[baseline_proportions == 0] = 0.000001
                    
                    # Calculate PSI
                    psi_col = (comparison_proportions - baseline_proportions) * np.log(comparison_proportions / baseline_proportions)
                    df_proportions['psi'] = psi_col
                    psi = np.sum(psi_col)

                    # save the outcome
                    var_psi[brand][key] = psi
                    tab_psi = pd.concat([tab_psi, df_proportions])
            
        tab_psi['value_bin'] = tab_psi['value_bin'].astype(str)
        tab_psi['run_date'] = date_of_interest
        tab_psi['count_base'] = tab_psi['count_base'].astype('double')
        
        self.before_loading_data_to_table("historical_psi_distribution")
        self.load_pandas_df_to_databricks_db(df=tab_psi, tab_name_in_databricks_db='historical_psi_distribution', write_mode='append', index=False, save_as_format='delta')
        for brand in ['nf', 'qb']:
            for key, value in var_psi[brand].items(): 
                if var_psi[brand][key] > threshold:
                    print(f'psi of variable {key}: ', var_psi[brand][key])
                    display(tab_psi[(tab_psi['brand']==brand)&(tab_psi['variable']==key)].sort_values('psi', ascending = False))

                    self.load_pandas_df_to_databricks_db(df=tab_psi[(tab_psi['brand']==brand)&(tab_psi['variable']==key)].sort_values('psi', ascending = False),        tab_name_in_databricks_db='AMP_Monthly_Reporting_PSI', write_mode='append', index=False, save_as_format='parquet')

    def model_score_distribution_comparison(self, month1, month2):

        try:
            spark.sql("DROP TABLE nf_workarea.AMP_Monthly_Reporting_Score_Distribution")     
        except:
            pass   

        mth1 = spark.sql(f"SELECT nf_scorebin_RESP, nf_scorebin_SUB, nf_mdl_name_RESP, nf_mdl_name_SUB FROM nf_workarea.AMP_score_history_v6 where run_date = '{month1}'").toPandas()
        mth2 = spark.sql(f"SELECT nf_scorebin_RESP, nf_scorebin_SUB, nf_mdl_name_RESP, nf_mdl_name_SUB FROM nf_workarea.AMP_score_history_v6 where run_date = '{month2}'").toPandas()

        stats_df = pd.DataFrame(columns = ['measure', 'scorebin', 'curr_count', 'curr_pct', 'prev_count', 'prev_pct'])

        for meas in ['RESP', 'SUB']:
            mth1_counts = mth1.groupby([f'nf_mdl_name_{meas}', f'nf_scorebin_{meas}']).size().reset_index().rename(columns = {0: 'curr_count', f'nf_scorebin_{meas}': 'scorebin', f'nf_mdl_name_{meas}': 'measure'})
            mth1_counts['curr_total'] = mth1_counts.groupby('measure')['curr_count'].transform('sum')
            mth1_counts['curr_pct'] = mth1_counts['curr_count'] / mth1_counts['curr_total']

            mth2_counts = mth2.groupby([f'nf_mdl_name_{meas}', f'nf_scorebin_{meas}']).size().reset_index().rename(columns = {0: 'prev_count', f'nf_scorebin_{meas}': 'scorebin', f'nf_mdl_name_{meas}': 'measure'})
            mth2_counts['prev_total'] = mth2_counts.groupby(f'measure')['prev_count'].transform('sum')
            mth2_counts['prev_pct'] = mth2_counts['prev_count'] / mth2_counts['prev_total']

            stats_meas = mth1_counts.merge(mth2_counts, on=['measure', 'scorebin']).sort_values(['measure', 'scorebin'])
            stats_meas['change_pct'] = (stats_meas['curr_count'] - stats_meas['prev_count'])/stats_meas['prev_count']
            stats_df = pd.concat([stats_df, stats_meas])

        display(stats_df[['measure', 'scorebin', 'curr_count', 'curr_pct', 'prev_count', 'prev_pct', 'change_pct']])
        self.load_pandas_df_to_databricks_db(df=stats_df, tab_name_in_databricks_db='AMP_Monthly_Reporting_Score_Distribution', write_mode='append', index=False, save_as_format='parquet')

    def file_exists(self, dir):
        try:
            dbutils.fs.ls(dir)
        except:
            return False  
        return True

# COMMAND ----------

