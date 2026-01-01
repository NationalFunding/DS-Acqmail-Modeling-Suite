import functools
import itertools
import os
import re
import shutil
from datetime import datetime
from glob import glob
from pathlib import Path, WindowsPath
from s3path import S3Path
from zipfile import ZipFile
#import boto3

import numpy as np
import pandas as pd
import scipy
from dateutil.relativedelta import relativedelta
from openpyxl.utils import get_column_letter

from config.deduplication import (COMPANY_LEGAL_STATUSES,
                                  STREET_NAME_DIRECTIONALS,
                                  STREET_NAME_POST_TYPES)
from config.run_env import (DNB_SELECTED_DATA_FILE, STAGING_FOLDER)
from config.final_file import MANN_WHITNEY_U_REPRESENTATIVE_SPLIT_CONFIDENCE, RANDOM_ASSIGNMENT_DIST_CHECK_THRESH
from config.run_env import (AWS_PROFILE_NAME, ARCHIVE_BUCKET_NAME)



class DataHandler:
    """
    Handler for Data Reading and writing operations. To be used by the Session class with the `.data` accessor. 
    """

    def __init__(self, session_obj):
        """
        Handler for data operations. To be used by the Session class with the `.data` accessor. 
        """

        self.__session = session_obj
        self.__session.log.info(f"Initializing DataHandler")
        self._data = {}
        self._rng = np.random.default_rng(seed=12345)

        # # Initialize the boto3 session to use the correct profile
        # self._aws_profile_name = AWS_PROFILE_NAME + ('-prod' if self.__session.running_in_prod else '-dev')
        # boto3.setup_default_session(profile_name=self._aws_profile_name)

    def s3_archive_all(self, folder_name, df, tab_name, mode, compression, skip_keys=None):

        bucket_name = ARCHIVE_BUCKET_NAME + ('-prod' if self.__session.running_in_prod else '-dev')
        save_folder = S3Path(f"/{bucket_name}/data-archive/{folder_name}")

        file_path = save_folder / (tab_name + '.parquet')
        self.__session.log.info(f'Archiving data at {save_folder}')
        df.to_parquet(file_path.as_uri(), compression=compression, storage_options={'profile':self._aws_profile_name}) 

        self.__session.log.info(f'Finished archiving {tab_name}')

    @staticmethod
    def is_same_prob_mass(series_1, series_2, threshhold):
        """
        A function to check if two passed probability masses have the same distribution.
        Uses the wasserstein_distance.

        Parameters
        ----------
        series_1 : pd.Series
            The first series to compare.
        series_2 : pd.Series
            The second series to compare.
        significance_level : float
            The significance level for the test. Default is 0.05.

        Returns
        -------
        bool
            True if the two series have the same distribution, False otherwise.
        """

        if not np.isclose(series_1.sum(), 1):
            raise ValueError('series_1 must be a PDF (sum to 1)')
        if not np.isclose(series_2.sum(), 1):
            raise ValueError('series_2 must be a PDF (sum to 1)')

        distance = scipy.stats.wasserstein_distance(series_1, series_2)

        return distance < threshhold

    def is_uniform(self, series, threshhold):
        """
        A function to check if a passed PDF is uniform.

        Parameters
        ----------
        series : pd.Series
            A series of probabilities to check.
        
        threshold : float
            The threshold to use for the Kolmogorov-Smirnov test.
        
        Returns
        -------
        bool
            True if the series is uniform or very close to it, False otherwise.
        """

        if len(series) < 2:
            return True
        
        # Generate a uniform distribution
        uniform = pd.Series(np.ones(len(series)) / len(series))

        return self.is_same_prob_mass(series, uniform, threshhold)

    def get_representative_partitions(self, df, partition_dist, method, sort_fields=None, check_fairness=True):
        """
        Use this to partition a dataframe, df, into num_partitions number of partitions that are as similar as possible.
        
        Parameters
        ----------
        df : pandas.DataFrame
            The dataframe to partition.
        partition_dist : int or pd.Series
            If int, the number of uniformly distributed partitions to create.
            If pd.Series, the distribution of partitions to create. 
                The index should be the partition number, and the values should be the relative proportion of records in each partition.
                i.e. the PDF of the distribution of records by partition.
        method : str
            The method to use to partition the dataframe. Valid values are 'random' and 'consecutive_assignment'.
            'random' will randomly assign rows to partitions. 
            'consecutive_assignment' will sort the dataframe by the sort_fields, and then assign rows to partitions in alternating order.
        sort_fields : list of str, optional
            The fields to sort the dataframe by. Only used if method is 'consecutive_assignment' or check_fairness=True.
        check_fairness : bool, optional
            If True, will check that the partitions are as similar as possible by using the check_partition_fairness method with the check_fairness fields.

        Returns
        -------
        partition_indices : pd.Series
            A series with the same index as df, and values that are the zero-indexed partition number for each row.
        """

        if not (isinstance(partition_dist, int) or isinstance(partition_dist, pd.Series)):
            raise ValueError('partition_dist must be an int or a pd.Series')

        # quick return if no split required
        if isinstance(partition_dist, int) and partition_dist == 1:
            return np.array([df.index])
        
        if method == 'random':

            if isinstance(partition_dist, int):
                partition_dist = pd.Series(np.ones(partition_dist) / partition_dist, dtype=float)

            partition_indices = self._rng.choice(a = partition_dist.index, size=len(df), p=partition_dist.values, )
            partition_indices = pd.Series(partition_indices, dtype=int, index=df.index)

            assert self.is_same_prob_mass(
                partition_dist,
                partition_indices.value_counts(normalize=True),
                threshhold= RANDOM_ASSIGNMENT_DIST_CHECK_THRESH,
                ), f'Partition distribution is not the same as the requested distribution.'

        elif method == 'consecutive_assignment':

            if isinstance(partition_dist, pd.Series):
                raise NotImplementedError('Cannot use a custom partition distribution with the random method.')
            else:
                assert isinstance(partition_dist, int), 'partition_dist must be an int'

            # sort by the specified fields, so that we get a fair allocation
            df = df.sort_values(by = sort_fields)

            # get a discrete uniform dist random var to offset the consecutive assignment with
            # this is needed for cases when len(df) < num_partitions, and df is a partition of a larger df
            # if we always start assigning at partition 0, then 1, then 2, etc when df's are small, then
            # when we concatenate all the df's we find more records have lower partition assigned (0, 1, etc)
            # compare to higher values like (num_partitions-1, num_partitions-2) 
            random_offset = self._rng.integers(low=0, high=partition_dist, size=1)[0]
            partition_indices = pd.RangeIndex(start=random_offset, stop=random_offset+len(df)) % partition_dist
            partition_indices = partition_indices.to_series(index=df.index)

        if check_fairness:
            self.check_partition_fairness(
                    df=df,
                    partition_indices=partition_indices,
                    numeric_fields_to_check=sort_fields,
                    )

        return partition_indices

    def check_partition_fairness(self, df, partition_indices, numeric_fields_to_check):
        """
        Checks that the partitions are as similar as possible.
        Uses the Mann Whitney U test to compare the distributions of the numeric_fields_to_check between partitions.
        Set the MANN_WHITNEY_U_REPRESENTATIVE_SPLIT_CONFIDENCE config parameter to adjust the confidence level.
        Raises warning if the p-value of the test is less than the confidence level.
        
        Parameters
        ----------
        df : pandas.DataFrame
            The dataframe to partition.
        partition_indices : pd.Series
            A series with the same index as df, and values that are the zero-indexed partition number for each row
            i.e. the output of get_representative_partitions
        numeric_fields_to_check : list of str
            The fields to check for fairness.

        Returns
        -------
        None
        """

        if partition_indices.unique().shape[0] == 1:
            return

        partitions = df.groupby(partition_indices)
        # get pairwise combination of all permutations
        # so that we can compare each partition against all other partitions
        # for a simple case of 2 partitions (AB test), we are just comparing the two
        pairwise_partitions = itertools.combinations(partitions.groups, r = 2)

        for pairwise_partition in pairwise_partitions:
            
            assert len(pairwise_partition) == 2

            partition_A = partitions.get_group(pairwise_partition[0])
            partition_B = partitions.get_group(pairwise_partition[1])
            
            for field in numeric_fields_to_check:
                
                x = partition_A[field].astype(float)
                y = partition_B[field].astype(float)

                _, p_value  = scipy.stats.mannwhitneyu(
                    x = x,
                    y = y,
                    use_continuity = True,
                    alternative='two-sided',
                    axis=0,
                    method='auto',
                    nan_policy='propagate',
                    )

                subject = f"Checked partition fairness for field '{field}':" 
                subject = subject.ljust(56, ' ')
    
                if p_value < (1-MANN_WHITNEY_U_REPRESENTATIVE_SPLIT_CONFIDENCE):
                    self.__session.log.warning(f"{subject} FAIL (rejected H_0) with p-value {p_value:.4f}")

    def _normalize_series(self, series, stop_words):
        """Normalizes a series of strings by removing stop words and punctuation and converting to lower case"""

        normalized = \
            series\
            .str.lower()\
            .str.replace('[^\w\s]','', regex=True)\
            .str.replace(r'\b'+r'\b|\b'.join(stop_words)+r'\b', '', regex=True)\
            .str.replace('\s+',' ', regex=True)\
            .str.strip()  

        return normalized
