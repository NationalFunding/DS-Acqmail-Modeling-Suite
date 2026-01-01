# Databricks notebook source
Version  : v5 
Release  : March 26, 2021

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Features:
# MAGIC 1. Analytic Functions 
# MAGIC   - Personalized Collection Prioritization Model v1 (RISK)
# MAGIC   - Scorecard v2 (RISK)
# MAGIC     - New Features: 
# MAGIC       - Ability to add fixed variables. 
# MAGIC       - Ability to add custom bins for numeric attributes. 
# MAGIC       - Refit functionality that allows user to retrain scorecard model with desired attribute and allows custom binning for numeric attributes. 
# MAGIC       - Ability to add hold-out sample for testing. 
# MAGIC     - Bug fixes: 
# MAGIC       - Missing bin is created separately for categorical attributes. 
# MAGIC       -Option of spark-based scoring function
# MAGIC - Analytics Studio Visual Designer - v1
# MAGIC   - We have added a UI capability to Studio called MLFlow Analytics Studio that allows users who do not have a data science background to leverage the analytical functions effortlessly. Through UI, the user can enjoy the same advanced functionalities of our functions minus the need to interact with any code. However, it is useful to remember that the Databricks notebook level functionalities are still available.
# MAGIC
# MAGIC   - This release comes with these Visual Designer Apps:
# MAGIC       - Propensity App (Marketing)
# MAGIC       - Scorecard App (Risk)
# MAGIC       - Credit Limit Assignment App (USA) (Risk)
# MAGIC   
# MAGIC   - Some features that enhance the user experience:
# MAGIC   - Users can upload a file directly by browsing their local file system. 
# MAGIC     - Results and reports are available at the click of a single button 
# MAGIC     - In Scorecard and Credit Limit Assignment UI, we have introduced a new data append functionality. The user can start with a file containing only DUNS and the target labels and now has complete visibility and flexibility of choosing what DNB data attributes go into the modelling process. 
# MAGIC     - In Propensity UI, the user has an array of options for scoring his/her addressable market. These include: 
# MAGIC       - Score only driver file DUNS 
# MAGIC       - Specify how many Prospect DUNS to be scored (if any) 
# MAGIC       - Include only those DUNS as Prospects that have a mailing address 
# MAGIC       - Include only those DUNS as Prospects that have a telephone number 
# MAGIC
# MAGIC - Sample data package v2 
# MAGIC   - Example data files are included now so sample notebooks will run using these files
# MAGIC
# MAGIC #### Data Bundle Releases:
# MAGIC | Bundle &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Use Case &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | GEO &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Experimental | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |-------|-------------|
# MAGIC | Country Risk Score MDA |RISK|US, INTL|Pilot| Multidimensional Country/Region Risk Scores |
# MAGIC | Insurance Renewal Proxy |MKT|US|Pilot| Insurance Renewal Proxy Scores |
# MAGIC | Global Risk Scores |RISK|INTL| &nbsp; | &nbsp; |
# MAGIC | Global Business Ranking |RISK | INTL |Pilot| GBR v2 (Machine Learning) |
# MAGIC | Web Domains Global |RISK, MKT|US, INTL| &nbsp; | &nbsp; |
# MAGIC | Failure Score Plus |RISK, MKT|US|Pilot| &nbsp; |
# MAGIC | Borrower Readiness Index |MKT|US|Pilot| Scores that assess the demand timing and need for borrowing |
# MAGIC | Hurricane Vulnerability | RISK  |US|Pilot| Predicts the likelihood of a business going Out of Business, Inactive, <br> or Bankrupt in the next 12 months following a hurricane. |
# MAGIC | Telemarketing | MKT | US | &nbsp; | &nbsp; |
# MAGIC | Foot Traffic | RISK, MKT | US | Pilot | Foot Traffic Data |
# MAGIC
# MAGIC
# MAGIC #### Data Bundle Updates:
# MAGIC - Revised local market customer bundles (on demand)
# MAGIC   - dnb_core_local_mkt_risk_scores_intl_limited by keeping only the restricted countries (Turkey, Italy, Vietnam)
# MAGIC   - dnb_core_local_mkt_legal_events_intl_limited by keeping only the restricted countries (Turkey, Italy, Vietnam)
# MAGIC   - dnb_core_local_mkt_trade_intl_limited by keeping only the restricted countries (Turkey, Italy, Vietnam)
# MAGIC   - dnb_core_local_mkt_financials_intl_limited by keeping only the restricted countries (Turkey, Italy, Vietnam)
# MAGIC   - dnb_core_local_mkt_risk_scores_intl by removing restricted countries (RISK) [INTL] 
# MAGIC   - dnb_core_local_mkt_trade_intl by removing restricted countries (RISK) [INTL]
# MAGIC   - dnb_core_local_mkt_legal_events_intl by removing restricted countries (RISK) [INTL]
# MAGIC   - dnb_core_local_mkt_financials_intl by removing restricted countries (RISK) [INTL]

# COMMAND ----------

