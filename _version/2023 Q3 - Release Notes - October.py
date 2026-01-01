# Databricks notebook source
Release  : October 7, 2023

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400"> </img>
# MAGIC
# MAGIC ## Analytic Functions
# MAGIC
# MAGIC
# MAGIC - Propensity/Response Enhancements:
# MAGIC   * Added the flexibility to choose any one of the CBS (TNS Communications BusinesScores) or ITBS (TNS IT BusinesScores) tables from TNS bundle in addition to the default basetable for US model geo.
# MAGIC   * Included shap bee swarm plot and its explanation in the propensity/response report. 
# MAGIC
# MAGIC - Scoring application (Propensity and Response):
# MAGIC   * Standalone scoring notebook which can be used for scoring the Propensity and Response (US, INTL and GLOBAL).
# MAGIC   * Scoring can be initiated for the given input file (OOT sample) with below parameters:
# MAGIC     * duns_column_name -> column name for the duns in the input file    
# MAGIC     * model_pickle_path -> pickle file path which contains the model to be used for scoring.   
# MAGIC   * Scoring can be done for any available load_year & load_month,  latest partition will be considered by default. 
# MAGIC   * Data sources which are considered for the given model in pickle file will be appended to the input file.
# MAGIC   * Scored table will be saved in the user workarea and the name will be logged to model_log_table for future reference.
# MAGIC
# MAGIC
# MAGIC ## Visual Designer
# MAGIC
# MAGIC
# MAGIC - Propensity/Response Enhancements:
# MAGIC   - Created a new widget to select Table Options with default basetable for US model geo. 
# MAGIC     * Selection Options for US Basetable (can choose only one) are:
# MAGIC       * Us_marketing_basetable with default basetable
# MAGIC       * Us_marketing_basetable + TNS ITBS 
# MAGIC       * Us_marketing_basetable + TNS CBS 
# MAGIC   - Included shap bee swarm plot and its explanation in the propensity/response report. 
# MAGIC
# MAGIC - Scoring application (Propensity and Response):
# MAGIC   * Developed a new application for Scoring the Propensity and Response (US, INTL and GLOBAL).
# MAGIC   * Created a dropdown to select only Propensity/Response pickle paths which displays latest pickles on top of dropdown list.
# MAGIC   * Added upload csv to upload DUNS and added text fields to input load year and load month which by default considers latest partition.
# MAGIC   * Displays output of top ten Scored sample table for US/INTL based on pickle path selected.
# MAGIC   * Also displays Scored Table Name for US/INTL.
# MAGIC
# MAGIC - Batch Extract 
# MAGIC   * Created a dropdown to select any one among Propensity, Response, Scorecard or Supplier Scorecard pickle paths built for US marketplace.
# MAGIC   * Added a new layout option - WorldBase (wb521) for INTL users.
# MAGIC
# MAGIC
# MAGIC ## Tools
# MAGIC
# MAGIC
# MAGIC - Batch Extract 
# MAGIC   * Annual Archive Essential Plus: Two new functions - Supplier Scorecard and Response has been introduced. 
# MAGIC   * Added a new layout - WorldBase (wb521) for INTL users.
# MAGIC
# MAGIC - Universal_Data_Appender_Utility
# MAGIC   * Robust solution to append data from multiple tables requiring only few input parameters.
# MAGIC   * Features:
# MAGIC     * Dynamically constructing queries from user input
# MAGIC     * Auto selecting distinct duns from each table
# MAGIC     * Converting SQL `decimal` type to Python friendly `double`
# MAGIC     * Remove duplicate column(s) that are present in multiple data tables
# MAGIC     * Check for the latest available partitions for each table they are appending
# MAGIC
# MAGIC
# MAGIC ## Core Data Releases
# MAGIC
# MAGIC
# MAGIC | Table Name &nbsp;  | Use Case &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|--------------|--------------|------------------|
# MAGIC |dnb_ucc_filings|	RISK, MARKETING|	US|	.|	This table is used to identify which of their customers/prospects have commercial transactions with a secured party.|
# MAGIC |dnb_ucc_collateral|	RISK, MARKETING|	US|	.|	This table is used to identify the collateral used for the commercial transactions.|
# MAGIC |dnb_ref_ucc_coll_code|	Reference|	US|	.|	A list of sample tables available in user_sample_data database.|
# MAGIC |dnb_ref_ucc_filing_type|	Reference|	US|	.|	A list of sample tables available in user_sample_data database.|
# MAGIC
# MAGIC

# COMMAND ----------

