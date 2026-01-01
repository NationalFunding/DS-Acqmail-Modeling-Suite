import pickle
import time
from pathlib import Path

import nltk
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from tqdm import tqdm

from config.run_env import (BUSINESS_NAME_LEMMATIZATIONS_FILE,
                              MARKETABLE_BUSINESS_CLASSIFIER_FILE)

class ModelHandler:

    def __init__(self, session_obj):
        """
        Handler for predictive modelling operations. To be used by the Session class with the `.model` accessor. 
        """

        self.__session = session_obj
        self.__session.log.info("Initializing ModelHandler")
        self.MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED = 160

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

        self.__session.log.info('Preprocessing business names: Removing numbers...')
        business_names = business_names.str.replace('\d+', 'num', regex=True)
        
        self.__session.log.info('Preprocessing business names: Removing punctuation...')
        business_names = business_names.str.replace('[^\w\s]', '', regex=True)
        
        self.__session.log.info('Preprocessing business names: Lowercasing...')
        business_names = business_names.str.lower()

        self.__session.log.info('Preprocessing business names: Lemmatizing...')
        
        lemmatizer=nltk.stem.WordNetLemmatizer()

        # C2 records with <NA> Business Name will be lemmatized to "", should be considered as not marketable
        # Should add a Market Insight filter to not export C2 records with no business name
        lemmatize_document = lambda doc: '' if pd.isnull(doc) else ' '.join([lemmatizer.lemmatize(w) for w in nltk.word_tokenize(doc)])
        
        if use_stored_lemmatizations:
            self.__session.log.info('Using stored lemmatizations...')

            try:
                name_mapper = pd.read_parquet(BUSINESS_NAME_LEMMATIZATIONS_FILE)['lemmatized_name']
                business_names_lemmed = business_names.map(name_mapper)
            
            except FileNotFoundError:
                self.__session.log.info(f'Unable to find stored lemmatizations file {BUSINESS_NAME_LEMMATIZATIONS_FILE}. Will lemmatize all business names and store them.')
                business_names_lemmed = pd.Series(index=business_names.index, dtype=business_names.dtype)

        else:
            self.__session.log.warning('NOT using stored lemmatizations. This will take a long time (~2-3 hours)...')
            business_names_lemmed = pd.Series(index=business_names.index, dtype=business_names.dtype)

        unlemmed_business_names = business_names_lemmed[business_names_lemmed.isna()]
        num_new_names_to_lemmatize = len(unlemmed_business_names)

        if num_new_names_to_lemmatize > 0:

            self.__session.log.info(f'Found {num_new_names_to_lemmatize:,} new business names to lemmatize. Starting now...')
            
            nltk.download('wordnet')
            nltk.download('punkt')
            nltk.download('omw-1.4')
            
            for index in tqdm(unlemmed_business_names.index, total=len(unlemmed_business_names), mininterval=1):
                business_names_lemmed.at[index] = lemmatize_document(business_names.at[index])        

            self.__session.log.info(f'Saving new business name lemmatizations to {BUSINESS_NAME_LEMMATIZATIONS_FILE}...')
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

        return business_names_lemmed     

    def marketable_business_classification(self, preprocessed_business_names, refit_model=False):
        """
        This function classifies business names as marketable or not marketable.

        Parameters
        ----------
        preprocessed_business_names : pd.Series
            The preprocessed business names to classify. These should be lemmed and lowercased.
        refit_model : bool
            If True, the function will refit the model.
        """

        self.__session.log.info('Loading marketable business classifier...')

        if not MARKETABLE_BUSINESS_CLASSIFIER_FILE.exists():
            self.__session.log_and_raise(FileNotFoundError, f'Unable to find marketable business classifier model file {MARKETABLE_BUSINESS_CLASSIFIER_FILE}')

        days_since_last_trained = self.check_time_since_model_trained(MARKETABLE_BUSINESS_CLASSIFIER_FILE)
        config_log_message = f'(MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED = {self.MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED})'
        if refit_model or (days_since_last_trained > self.MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED):
            self.__session.log.warning(f'Marketable business classifier model is {days_since_last_trained:,.0f} days old. It will be retrained. {config_log_message}')
            self.train_marketable_business_classifier()
        else:
            self.__session.log.info(f'Marketable business classifier model is {days_since_last_trained:,.0f} days old. It will be used. {config_log_message}')

        with open(MARKETABLE_BUSINESS_CLASSIFIER_FILE, "rb") as f:
            marketable_business_classifier = pickle.load(f)
        self.__session.log.info('Done loading marketable business classifier')
        self.__session.log.info("Predicting class 2 marketability...")
        predictions = marketable_business_classifier.predict(preprocessed_business_names)
        self.__session.log.info("Done predicting class 2 marketability")

        return predictions

    def train_marketable_business_classifier(self):
        """
        This function trains the marketable business classifier. 
        Called automatically if days_since_last_trained > MARKETABLE_BUSINESS_CLASSIFIER_MAX_DAYS_SINCE_TRAINED 
        """

        log_topic = "Training marketable business classifier... | "
        self.__session.log.info(log_topic)

        c1_raw_data_paths = [
            '//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/202206/Q1 MI Data Extract 2-6_10_2022.zip',
            '//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/202205/Q1 MI Data Extract 2-5_9_2022.zip',
            '//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/202204/Q1 MI Data Extract 2-4_11_2022.zip',
            '//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/202203/Q1 MI Data Extract 2-3_10_2022.zip',
            '//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/202202/Q1 MI Data Extract 2-2_9_2022.zip',
            '//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/202201/Q1 MI Data Extract 2-1_11_2022.zip',
        ]

        cols = ['duns', 'business_name', 'sic_8']
        df = pd.DataFrame(columns=cols)

        for file_path in c1_raw_data_paths:

            self.__session.log.info(log_topic + "Reading in file: " + file_path)

            df_temp = pd.read_csv(filepath_or_buffer = file_path,
                            delimiter = '|',
                            encoding = "ISO-8859-1",
                            header = 0,
                            usecols=["Duns Number", "Business Name", "Primary SIC 8 Digit"],
                            dtype=str,
                            quoting = 3,
                            )

            # read in data frame and append to master data frame
            # (we do one at a time to save memory since there will be many duplicates)
            df_temp.columns = cols
            df = pd.concat([df, df_temp], ignore_index=True)
            df = df.drop_duplicates(subset=['duns'], keep='first')

        self.__session.log.info(log_topic + "Done reading in files")

        df = df.dropna(subset='business_name')

        marketable_sic = self.__session.sql.read_script("sql_scripts/get_marketable_sics.sql", dtype=str)
        df['sic_4'] = df['sic_8'].str.slice(0, 4) 
        df['marketable'] = df['sic_4'].isin(marketable_sic['sic_4'])

        df['business_name_cleaned'] = self.preprocess_business_names(df['business_name'], use_stored_lemmatizations=True)

        ################################################################
        # MODEL ARCHITECTURE

        # count_vectorizer = CountVectorizer(
        #                             strip_accents='unicode',
        #                             ngram_range=(1, 2),
        #                             stop_words='english',
        #                             min_df=0.0001,
        #                             max_df=1.0,
        #                             max_features=2_500,
        #                             binary=True,
        #                             )

        # tf_idf_transformer = TfidfTransformer(
        #                             use_idf=False
        #                             )

        # linear_svm = SGDClassifier(
        #                         loss='hinge',
        #                         penalty='l1',
        #                         alpha=1e-4,
        #                         max_iter=40,
        #                         tol=1e-4,
        #                         random_state=42,
        #                         learning_rate='optimal',
        #                         )

        # marketable_business_classifier = Pipeline([
        #     ('count_vectorizer', count_vectorizer),
        #     ('tf_idf_transformer', tf_idf_transformer),
        #     ('linear_svm', linear_svm),
        # ])

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

        self.__session.log.info(log_topic + 'Done training model')
        self.__session.log.info(log_topic + 'Benchmarking model performance...')

        y_pred = marketable_business_classifier.predict(X_test)

        report = classification_report(
                            y_true = y_test,
                            y_pred = y_pred,
                            target_names= ['Restricted', 'Unrestricted'],
                            )

        self.__session.log.info(log_topic + 'Classification Report:\n' + report)


        self.__session.log.info(log_topic + 'Saving model...')
        
        with open(MARKETABLE_BUSINESS_CLASSIFIER_FILE, "wb") as f:
            pickle.dump(marketable_business_classifier, f, pickle.HIGHEST_PROTOCOL)

        self.__session.log.info(log_topic + 'Done saving model')

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

        if model_file_path.exists():
            last_modified = model_file_path.stat().st_mtime
            now = time.time()
            seconds_since_last_modified = now - last_modified
            days_since_last_modified = seconds_since_last_modified / (60 * 60 * 24)
            return days_since_last_modified
        else:
            return None


