# Databricks notebook source
Release  : March 25, 2022

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400">
# MAGIC
# MAGIC <h4> 1) Analytic Functions </h4>
# MAGIC
# MAGIC   - Global Credit Scorecard (US, Global)(RISK)
# MAGIC     - New Features: 
# MAGIC       - Enhanced Scorecard Binning and Feature Selection Process.
# MAGIC       - Added Train-Test Split Indicator
# MAGIC       - Supports 100% Training Sample and separate validation/test set
# MAGIC       - Functionality to use additional side file/data along with driver file/data.
# MAGIC       - Functionality to append Alternate Data Sources along with other data sources. 
# MAGIC       <br>
# MAGIC       
# MAGIC   - Global Credit Limit Assignment CLA (US, Global)(RISK)
# MAGIC     - New Features:
# MAGIC       - All the new Scorecard enhancements are enabled in CLA. 	 
# MAGIC       <br>
# MAGIC       
# MAGIC   - Propensity Model (US, Global) (MKT)
# MAGIC     - New Features: 
# MAGIC       - Expanded the Propensity model into Global portfolio use case. 
# MAGIC       - Added model segmentation into US and Intl markets. 
# MAGIC       - Functionality to use additional side file data along with driver file data. 
# MAGIC       - Functionality to append Alternate Data Sources along with other data sources. 
# MAGIC       - During data append given more priority to side file data duns. 
# MAGIC 	  - Standardized naming convention for base table. <code> Eg : dnb_basetable_usa_{base_table_load_year}_{base_table_load_month} </code>   
# MAGIC       
# MAGIC       
# MAGIC   - AutoML (MKT, RISK)
# MAGIC     - New Features:
# MAGIC       - Functionality to do stratify split of input data into train-test data based on multiple columns (for binary classification)
# MAGIC       - Functionality to control K-Fold cross validation using a parameter (for binary classification)
# MAGIC       - Functionality to see the 'reason to drop' for each variable dropped during eda/feature-selection (for binary classification)
# MAGIC       - Functionality to use categorical variables for LightGBM and Catboost (for binary classification)
# MAGIC       - Functionality to display train and test data used during hyper-parameter tuning and model building. 
# MAGIC       - Functionality to Supports 100% Training Sample and separate validation/test set
# MAGIC       - Functionality to score on unseen data and get 'model_score' along with 'probability'
# MAGIC       - Functionality to log the particular iteration's artifacts using mlflow 
# MAGIC       <br>
# MAGIC       
# MAGIC   - DEX Sampler Capabilities
# MAGIC     - The DEX Sampler Utility allows the customers to select an Alt. Data source from the Marketplace snowflake platform to get an adhoc sample dataset (with upto    100K records) selected based on a DUNS input or random selection logic. 
# MAGIC     - Features:
# MAGIC       - Wrapper Notebooks inclusive of the Customer Parameters and functions calling the API supporting the DEX Sampler Utility. 
# MAGIC       - Ability to choose DUNS based on the input file/input table given by the customer or a random selection of DUNS performed internally.
# MAGIC       - Ability to read the config file of the customer to extract the enablement/disablement for the utility.
# MAGIC   
# MAGIC   - Batch Extract Capability
# MAGIC     - This gives the capability for adhoc data delivery for the following set of licensed layouts available for selection by the customer, namely: DUNS only, TPA, TPA+IT, SDMR.
# MAGIC     - Features:
# MAGIC       - Wrapper Notebooks inclusive of the Customer Parameters and functions calling the API supporting the Batch Extract Utility. 
# MAGIC       - Ability to read the config file of the customer to extract the type of layout subscribed by the customer for the utility.
# MAGIC
# MAGIC <h4> 2) Analytics Studio Visual Designer - v3 </h4>
# MAGIC
# MAGIC   - This release comes with these Visual Designer Apps:
# MAGIC       - Global Credit Scorecard (USA, INTL, Global)(RISK)
# MAGIC         - Option to upload a side file with duns as a mandatory column or a table in data bricks.
# MAGIC         - Option to choose maximum of 3 alternative data sources.
# MAGIC         - Improved User Experience with 'tab' functionality.
# MAGIC         - Added URL of job running in backend of UI.
# MAGIC         
# MAGIC         
# MAGIC       - Global Credit Limit Assignment (USA, INTL, Global) (RISK)
# MAGIC         - Option to upload a side file with duns as a mandatory column or a table in data bricks.
# MAGIC         - Option to choose maximum of 3 alternative data sources.
# MAGIC         - Improved User Experience with 'tab' functionality.
# MAGIC         - Added URL of job running in backend of UI.
# MAGIC         
# MAGIC         
# MAGIC       - Propensity Model UI (USA, INTL, Global) (MKT)
# MAGIC         - Option to upload a side file with duns as a mandatory column or a table in data bricks.
# MAGIC         - Option to choose maximum of 3 alternative data sources.
# MAGIC         - Enhanced UI for the Propensity model to support Global portfolio. 
# MAGIC         - Option to choose base_table dynamically from the drop-down.
# MAGIC         - Option to rebuild base table with standardized naming convention for base table.
# MAGIC         - Improved User Experience with 'tab' functionality.
# MAGIC         - Added URL of job running in backend of UI.
# MAGIC         
# MAGIC         
# MAGIC       - AutoML UI (MKT, RISK)
# MAGIC         - Decommissioned. Automl UI is not applicable anymore.
# MAGIC    
# MAGIC          
# MAGIC <h4> 3) Batch Model Automation </h4>
# MAGIC  
# MAGIC   - This release comes with the Batch Model Automation of Deployment:
# MAGIC       - Based on the enhancements in model building process, Model Scoring notebooks have been updated.
# MAGIC        
# MAGIC
# MAGIC <h4> 4) Data Table Releases: </h4>
# MAGIC
# MAGIC | Table Name &nbsp;  | Use Case &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|--------------|--------------|------------------|
# MAGIC |dnb_supply_chain_aggregates_global|	SUPPLY RISK|	Global| .| 	This table contains a series of compliance, financial, operational and reputational risk factors associated with a given Tier-1 supplier and the aggregated number of critical/noncritical Tier-2 and Tier-3 suppliers with each high risk factor in D&B Supply Chain Network. D&B Supply Chain Network collects 60 million+ supplier-buyer relationships from trade data, inquiry data, shipping data and website data. Estimated spend between buyer and supplier is used to identify critical suppliers (estimated spend>$100,000, by default). D&B corporate hierarchy is used to capture supplier relationships of branches and subsidiaries majority owned by a business. |
# MAGIC |dnb_da_supply_chain_network_global|	SUPPLY RISK|	Global|	.|	This table contains Tier-2 and Tier-3 suppliers of a given Tier-1 supplier in D&B Supply Chain Network. D&B Supply Chain Network collects 60 million+ supplier-buyer relationships from trade data, inquiry data, shipping data and website data. For each supplier DUNS (Tier-1,2,3), a series of compliance, financial, operational and reputational risk factors are presented to identify risks for different use cases. Estimated spend between buyer and supplier is presented and used to identify critical suppliers (estimated spend>$100,000, by default). For non-disclosable relationships (disclosable_indicator=0), DUNS number is tokenized and the business name, address, phone are omitted. .|
# MAGIC |dnb_da_shipping_global|RISK, MKT, SUPPLY|	Global|	Pilot|	This data contains a series of derived attributes for each DUNS number in D&B shipping data including: role indicator: is shipper, consignee, forwarder, requester, contract party, notify party, logistic company, exporter, importer;volume: number of packages, weight, estimated value, TEU (number of twenty-foot equivalent container units);dependency: number of companies, countries, Harmonized System (HS) codes importing/exporting and percentage of TEU import from/export to China, USA, UK, EU;delay: average delay days (import/export);product: top 3 Harmonized System (HS) codes. The attributes are created each month for last 1,3,6,9,12,18,24 months shipping data aggregated. Historical derived attributes is available from 2016/01.|
# MAGIC |dnb_ref_duns_uei|	RISK, MKT, SUPPLY|	Global|	.|	The UEI (SAM) is a unique entity ID generated in the System for Award Management (SAM.gov), as the official identifier for doing business with the government beginning in April 2022.|
# MAGIC |dnb_webdomain_global|	RISK, MKT|	Global|	.|	Web presence information, now partitioned by load_date.|
# MAGIC |dnb_ref_country_region|	RISK, MKT, SUPPLY|	Global|	.|	Cross Reference for D&B Country/Region Codes, ISO Country/Region Codes and FIPS Country/Region Codes.|
# MAGIC |dnb_macro_risk_factors_us|	RISK, MKT, SUPPLY|	US|	Pilot|	US State and Industry Level Risk Classifications Based on Macroeconomic Risk Factors .|
# MAGIC |dnb_macro_risk_factors_intl|	RISK, MKT, SUPPLY|	INTL|	Pilot|	International State and Industry Level Risk Classifications Based on Macroeconomic Risk Factors .|
# MAGIC |dnb_studio_product_crosswalk|	REFERENCE|	Global|	.|	A crosswalk table between studio data tables/columns with other D&B products.| 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC <h4> 5) Alternative Data Release </h4>
# MAGIC
# MAGIC | Bundle &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|
# MAGIC |gravyanalytics_visitation_count|Weekly visitation count data when a mobile device opted into location services is observed at a commercial place of interest|
# MAGIC |rel8ed_license|Rel8ed.to's Permits and License data. Use the data to identify newly-licensed prospects, growing market sectors, and non-compliance.|
# MAGIC |apatics_provider_master|This table contains data elements of Master Records for Medical Providers.|

# COMMAND ----------

