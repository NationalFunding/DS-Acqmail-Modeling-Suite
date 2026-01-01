import logging
import sys
from datetime import datetime

import sklearn
from dateutil.relativedelta import relativedelta

from config.run_env import DATA_EXPORTS_FOLDER, MAIL_COUNTS_FOLDER
from config.run_env import RUN_DATE_OVERRIDE, RUN_IN_PROD, ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE
from handlers.data import DataHandler
#from handlers.ftp import FTPHandler
from handlers.model import ModelHandler
from handlers.sql import SQLHandler
from handlers.snowflake import SnowflakeHandler
from handlers.teams import TeamsHandler


class Session:

    def __init__(self, use_log:bool=True):
        """
        An object to manage global-ish variables, logging, config, and other session-related functions for run scripts 

        Attributes
        ----------
        log : logging.Logger
            The logger object for the session.
        run_dt : datetime.datetime
            The datetime of when the session was initiated.
        mail_dt :The mail date for the session
        running_in_prod : bool
            Whether the session is running in production or not.
        
                
        Accessors
        ---------
        ftp : FTPHandler
            Internal FTPHandler object used to upload files to FTP servers.
            See handlers/ftp.py for implementation.
        sql : SQLHandler
            Internal SQLHandler object used to interact with SQL databases.
            See handlers/sql.py for implementation.
        snowflake : SnowflakeHandler
            Internal SnowflakeHandler object used to interact with Snowflake databases.
            See handlers/snowflake.py for implementation.
        model : ModelHandler
            Internal ModelHandler object used to interact with predictive models.
            See handlers/model.py for implementation.
        data : DataHandler
            Internal DataHandler object used to interact with data.
        teams : TeamsHandler
            Internal Teams object used to make automated Teams messages when the script is run.
            See handlers/teams.py for implementation.
        
        Methods
        -------
        log_and_raise(exception, message=None)
            Logs the exception and message to the logger and raises the exception for convenience.

        """
        self._check_environment()
        
        self.use_log = use_log
        self._initialize_logger()

        self.log.info('Initializing Session')

        self._initialize_config()

        self._initialize_date_information()

        self._initialize_secrets()

        self._initialize_handlers()

        ######### Set Folder Paths and File Names #########
        self.data_exports_folder = DATA_EXPORTS_FOLDER / self.run_dt.strftime('%Y%m') 
        self.mail_counts_folder = MAIL_COUNTS_FOLDER / self.mail_dt.strftime('%Y') / self.mail_dt.strftime('%Y-%m') / "Acquisition"

    def log_and_raise(self, exception, message=None):
        
        logger_message = f'{type(exception).__name__}: {exception}'
        
        if message:
            logger_message = f'{message} -- {logger_message}'
        
        self.log.error(logger_message)

        raise exception
    
    def _initialize_logger(self):

        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler = logging.FileHandler(datetime.now().strftime('AMP-log-%Y-%m-%d.log'))
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        self.log = logging.getLogger('AMP-log')
        self.log.addHandler(handler)
        self.log.setLevel(logging.DEBUG)

        self.log.disabled = not self.use_log

    def _initialize_date_information(self):

        if (ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE or not self.running_in_prod) and RUN_DATE_OVERRIDE:
            self.log.warning(f"Overriding run date with {RUN_DATE_OVERRIDE}")
            self.run_dt = datetime.strptime(RUN_DATE_OVERRIDE, '%Y-%m-%d')
        elif self.running_in_prod and RUN_DATE_OVERRIDE and not ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE:
            self.log_and_raise(ValueError(f"Cannot override run date in production environment without ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE set to True in the config/session.py file."))
        else:
            self.run_dt = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        self.mail_dt = self.run_dt + relativedelta(months=2)
        self.prior_dt = self.run_dt + relativedelta(months=-1)

    def _initialize_config(self):
        
        self.running_in_prod = RUN_IN_PROD

        if self.running_in_prod:
            self.log.warning(f"! Using PRODUCTION Environment !")
        else:
            self.log.info(f"Using Development Environment")

    def _initialize_secrets(self):

        import credentials
        self._credentials = credentials

    def _initialize_handlers(self):

        #self.ftp = FTPHandler(self)
        self.sql = SQLHandler(self)
        self.model = ModelHandler(self)
        self.data = DataHandler(self)
        self.snowflake = SnowflakeHandler(self)
        self.teams = TeamsHandler(self)

    def _check_environment(self):
        """
        Checks that the environment is set up correctly for the session.
        """

        assert sys.version_info.major == 3, "Must be using Python 3"
        assert sys.version_info.minor >= 10, "Must be using Python 3.10 or newer"
        sklearn_major, sklearn_minor, _ = sklearn.__version__.split('.')
        assert int(sklearn_major) == 1 and int(sklearn_minor) >= 1, "Must be using Scikit-Learn 1.1 or newer"