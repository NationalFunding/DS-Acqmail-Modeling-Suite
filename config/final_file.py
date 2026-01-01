# These are account IDs that should not be assigned in the future.  
# If these are values are ever initially assigned to a record, then they will automatically be reassigned to the next available account ID.
# The first two values here are values used as account IDs from 2019-09 which did not follow the regular consecutive numbering scheme,
# and instead skipped ahead very far. To make sure these values are not assigned, they are added to this list. 
RESTRICTED_ACCOUNT_IDS = [442345678, 332345678]

# Max allowable Wasserstein distance between the distribution of 
# of campaign arms in the campaign table and the distribution of
# the records assigned to each campaign arm. 
RANDOM_ASSIGNMENT_DIST_CHECK_THRESH = 0.03

# Max allowable Wasserstein distance between the uniform distribution
# and the distribution of the records in each campaign arm
CAMPAIGN_ASSIGNMENT_UNIFORMITY_CHECK_THRESH = 0.03

# Max allowable Wasserstein distance between the uniform distribution
# and the distribution of the records in each drop 
DROP_ASSIGNMENT_UNIFORMITY_CHECK_THRESH = 0.04




# Confidence level for the Mann Whitney U test used to split data
# into fair, representative partitions. 
# If the resulting P-value of the test between two partitions 
# is less than 1 - MANN_WHITNEY_U_FAIR_SPLIT_CONFIDENCE then 
# we reject the null hypothesis that the partitions come from 
# the same distribution
# If you want to be more lenient about differences in partitions, then use a greater value. 
# If you want to more strictly enforce that each partition is distributed similarly, use a lower value.
MANN_WHITNEY_U_REPRESENTATIVE_SPLIT_CONFIDENCE = 0.95

BLIND_SEEDS = [
    {
        "full_name": "Torbjorn Bergstrand",
        "business_name": "Torbjorn Consulting",
        "address": "724 Daylight Drive",
        "city": "Maryville",
        "state": "TN",
        "zip_code": 37801,
    },
    {
            "first_name": "Richard",
            "last_name": "Mathews",
            "full_name": "Richard Mathews",
            "business_name": "Mathews Consulting",
            "address": "28877 Pujol Street, Apt. 1533",
            "city": "Temecula",
            "state": "CA",
            "zip_code": 92590,
    }, 
]

REP_NAME = ""
REP_PHONE = "888-733-9483"