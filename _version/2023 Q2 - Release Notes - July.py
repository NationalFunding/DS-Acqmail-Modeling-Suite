# Databricks notebook source
Release  : July 7, 2023

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400"> </img>
# MAGIC
# MAGIC # Core Features
# MAGIC
# MAGIC ## Training Folder Structure Changes
# MAGIC * Root folder is moved from `/training` to `/__Training`
# MAGIC * there will be 4 sub-folders by categories:
# MAGIC   * `/__Training/D&B_Core`
# MAGIC   * `/__Training/D&B_Marketing`
# MAGIC   * `/__Training/D&B_Risk`
# MAGIC   * `/__Training/D&B_TPRC`
# MAGIC * To run notebook(s) in the training folder, clone the following to your personal workspace:
# MAGIC   * clone `/__Training/D&B_Core` (all customers)
# MAGIC     * clone `/__Training/D&B_Marketing` (Marketing customer only)
# MAGIC     * clone `/__Training/D&B_Risk` (Risk customer only)
# MAGIC     * clone `/__Training/D&B_TPRC` (TPRC customer only)
# MAGIC
# MAGIC ## Analytic Functions
# MAGIC - Batch Extract 
# MAGIC   * Introducing a new capabilty in Batch Extract application to also extract Records Under Management (RUM)
# MAGIC   * Customer can now choose the type of extract - RUM or Both based on the subscribed extract layouts and corresponding license tracking. 
# MAGIC   * Annual Archive Essential Plus code optimized.
# MAGIC
# MAGIC - Scoring application (scorecard):
# MAGIC   * Standalone scoring notebook which can be used for scoring the standard scorecard (US & INTL).
# MAGIC   * Scoring can be initiated for the given input file (OOT sample) with below parameters:
# MAGIC     * duns_column_name -> column name for the duns in the input file    
# MAGIC     * model_pickle_path -> pickle file path which contains the model to be used for scoring.   
# MAGIC   * Scoring can be done for any available load_year & load_month,  latest partition will be considered by default. 
# MAGIC   * Data sources which are considered for the given model in pickle file will be appended to the input file.
# MAGIC   * Scored table will be saved in the user workarea and the name will be logged to model_log_table for future reference.
# MAGIC
# MAGIC ## Visual Designer
# MAGIC - Batch Extract 
# MAGIC   * Now the customer can only extract the layout they have subscribed for and they wont be able to proceed with the application stages for unsubscribed layouts.
# MAGIC   * License tracking widgets has been introduced which will enable user to select based on the chosen layout.
# MAGIC       * A non-editable widget is displayed for customer information if they have subscribed for either RUM or Record (no customer input is required)
# MAGIC       * Radio button is displayed if the customer has subscribed for both - RUM & Record (default option is both)
# MAGIC
# MAGIC - Quota Check Application
# MAGIC   * This new application enables customer to check their quota status for subscribed extract layouts
# MAGIC
# MAGIC - Scoring application (scorecard):
# MAGIC   * Developed a new application for Scoring the standard scorecard (US & INTL).
# MAGIC   * Created a dropdown to select only scorecard pickle paths which displays latest pickles on top of dropdown list.
# MAGIC   * Added upload csv to upload DUNS and added text fields to input load year and load month which by default considers latest partition.
# MAGIC   * Displays output of top ten Scored sample table for US/INTL based on pickle path selected.
# MAGIC   * Also displays Scored Table Name for US/INTL.
# MAGIC
# MAGIC ## Core Data Releases
# MAGIC
# MAGIC | Table Name &nbsp;  | Use Case &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|--------------|--------------|------------------|
# MAGIC |dnb_tns_ebf| RISK, MARKETING, SUPPLY|	US| .| 	TNS Enhanced Building File.|
# MAGIC |dnb_tns_itbs| RISK, MARKETING, SUPPLY|	US| .| 	TNS IT BusinesScores.|
# MAGIC |dnb_tns_cbs| RISK, MARKETING, SUPPLY|	US| .| 	TNS Communications BusinesScores.|
# MAGIC |dnb_tns_duns_to_ebf| RISK, MARKETING, SUPPLY|	US| .| 	TNS Crosswalk from DUNS to EBF CPL Code.|
# MAGIC |dnb_tns_cpl|	RISK, MARKETING, SUPPLY| US| .| 	TNS Crosswalk from DUNS to D&B CPL Code.|
# MAGIC |dnb_da_netwise_contact_us| RISK, MARKETING|	US| .| 	This table is an aggregate of contact information at the DUNS level. It is used to identify the segmentation of roles and seniority of contacts within a given DUNS number along with email address and LinkedIn coverage information.|
# MAGIC |dnb_dm_supplier_global|SUPPLY|	GLOBAL|	.|	Table for managing and organizing supplier data. |
# MAGIC |sample_table_summary|	Reference|	GLOBAL|	.|	A list of sample tables available in user_sample_data database.|
# MAGIC
# MAGIC ## Following tables have been dropped from all customer workspaces
# MAGIC
# MAGIC |Table Name &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC -------------|--------------|--------------|------------------|
# MAGIC | dnb_da_spend_hq_us|	US| .| 	D&B Business Predictors aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC | dnb_borrowing_capacity_index_us|	US| .| 	D&B Borrowing Capacity Index provides ability to quantify risk level and assess small business borrowing capacity.|

# COMMAND ----------


