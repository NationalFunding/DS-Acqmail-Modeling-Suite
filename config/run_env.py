from pathlib import Path

### Run Env
RUN_IN_PROD = False
RUN_DATE_OVERRIDE = '2025-12-01'
SAVE_FILE_COMPRESSION = None
SNOWFLAKE_HIGHTOUGH_ROLE = False

# must be true if running in prod and overriding the run date
# (just as a confirmation that you know what you're doing)
ACKNOWLEDGE_PROD_RUN_DATE_OVERRIDE = False

# For certain months, EXL doesn't do any selections for SBL, hence this variable was made in order to ignore SBL in 05b_EXL_archive if that happens
EXL_BRANDS_MAILED = ['NF', 'QB']

# Count of NF-NF and QB-QB testing
DOUBLE_SAME_BRAND_COUNT = 180000

# DS PPP Segment
DS_PPP = True

# Illinois Compliance
ILLINOIS_COMPLIANCE = True

# SIC2 Testing
SIC2_TESTING = False
SIC2_CREATIVE_LIST = ['15 Gen Contractors', '17 Spec. Contractors', '42 Truckers', '58 Restaurants']

# Segmentation based on previous campaign
PREVIOUS_MONTH_SEGMENTATION = False
PREVIOUS_MONTH_SEGMENTATION_NAME = 'Not in NOV Test'

NF_CONTROL_GROUPS = ['Model V5 Test', 'Single Mail - from QB']
QB_CONTROL_GROUPS = ['Double Mail - QB & SBL']
SBL_CONTROL_GROUPS = []
NON_RELATED_GROUPS = ['EXL New - IL Compliance', 'EXL New - List 1', 'EXL Prescreen', 'Prescreen Double Mail BAU', 'Equifax Prescreen', 'Prescreen Double Mail NF+NF', 'Prescreen Single', 'Prescreen Trigger']

# Selection File
EXL_SELECTION_FILE = ["NFI_NFI_PREFL_8539_2507D_U_20250609140835.txt", "NFI_NFI_PREFL_8854_2507M_U_20250528122832.txt"]
NON_EXL_SELECTION_FILE = "Salesforce_table_Prescreen_062025.csv"

### SQL Database
DEV_SERVER = "S26"
PROD_SERVER = "S28"
UID = "svc_rmodel"

AWS_PROFILE_NAME = 'acq-mail' # appended with either '-dev' or '-prod' depending on environment
ARCHIVE_BUCKET_NAME = 'nationalfunding-acquisition-mail' # appended with either '-dev' or '-prod' depending on environment

# Snowflake Info
SNOWFLAKE_USER = "GNOLASCO@FAIRSQUARE.COM"
SNOWFLAKE_DEV = {'server': 'zvb02300', 'warehouse': 'DATASCIENCE'}
SNOWFLAKE_PROD = {'server': 'idb59911', 'warehouse': 'SNOWPARK_MEDIUM'}

### Model
MODEL_VERSION = "ResponsePlus_v4.1.0"

### Slack
# list of slack channel ids to send updates to. These are user-friendly messages for broadcasting when a job is done (for the marketing team).
# Can be slack channel id or slack user id.
UPDATE_DESTINATIONS = ["C035Y5Y0B25"]
# list of slack ids for users to receive log files and messages about errors. These are technical messages for the data science team.
# Can be slack channel id or slack user id.
DEBUG_DESTINATIONS = ["U02GJ0VM1G8", "U052XJQNS8K", "U08PBFT113K"]


### FTP
HOST = {
    "nf": "portal.nationalfunding.com",
    "dnb": "mftweb.dnb.com"
    }

FTP_UID = {
    "nf": "svc_maillist",
    "dnb": "natfndin"
    }
    
FROM_DNB = "DirectMailExtracts/DnB/ScheduledExtractLanding/Production"
FROM_SUMMIT = "Summit Direct Mail - Data/Files FROM Summit"
TO_SUMMIT = "Summit Direct Mail - Data/Files TO Summit"
EXL_FTP = "Data Science/Marketing/Data Vendor/EXL"
FROM_SBL = "SBL/direct_mail/data_pull"

SUPPRESSED_ADDRESSES = ['16000 Dallas Pkwy Ste 300']

#FOLDERS
################################################
# for saving data, and export location for when running in dev (not prod)
STAGING_FOLDER = "//corp/nffs/Departments/BusinessIntelligence/Data Science/Projects/Marketing/Acquistion Mail Staging/"
STAGING_FOLDER = Path(STAGING_FOLDER)

DATA_EXPORTS_FOLDER = "//corp/nffs/Departments/BusinessIntelligence/D&B/MI Data Exports/"
DATA_EXPORTS_FOLDER = Path(DATA_EXPORTS_FOLDER)

MAIL_COUNTS_FOLDER = "//corp/nffs/Departments/Marketing/Ada/Mail Counts/"
MAIL_COUNTS_FOLDER = Path(MAIL_COUNTS_FOLDER)

# where we save the Final Files
FINAL_FILE_FOLDER = "//corp/nffs/Departments/Marketing/Campaign Channels/Direct Mail/Acquisition/"
FINAL_FILE_FOLDER = Path(FINAL_FILE_FOLDER)

# where we save the Append Files
APPEND_FILE_FOLDER = "//corp/nffs/Departments/Marketing/Campaign Channels/Direct Mail/Acquisition/"
APPEND_FILE_FOLDER = Path(APPEND_FILE_FOLDER)

# where we save marketing models
MODEL_SAVE_FOLDER_NAME = STAGING_FOLDER / "models"


# FILES
################################################

BUSINESS_NAME_LEMMATIZATIONS_FILE = STAGING_FOLDER / "lemmatized_names.parquet"

MARKETABLE_BUSINESS_CLASSIFIER_FILE = MODEL_SAVE_FOLDER_NAME / "marketable_business_classifier_v1-0-0.pkl" # in staging folder

# this is the file name exported by Market Insight that contains all of the combined leads after selection and licensing
# on export, this will be zipped like "{DNB_SELECTED_DATA_FILE}.zip/{DNB_SELECTED_DATA_FILE}.txt"
# this name is set within the DNB Market Insight selection template
DNB_SELECTED_DATA_FILE = "selection_export"