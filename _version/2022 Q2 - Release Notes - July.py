# Databricks notebook source
Release  : July 1, 2022

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400"> </img>
# MAGIC
# MAGIC ## Core Features
# MAGIC - Studio Quarterly Config for Mid-Market customers
# MAGIC   * Lower price offering of Studio offering quarterly refresh interval.  Giving customers choice of quarterly vs monthly refresh.
# MAGIC - Compliance optimization for CCPA, GDPR and China regulations
# MAGIC - Load Month/Load Year Databricks Partitioning  
# MAGIC   * Performance optimization by creating workspace level date partitions. Reduces query execution time on select SQL queries (ex: single table, single partition etc)
# MAGIC - Storage Quota Enhancement: 
# MAGIC   * Enhancements to display disk storage use (shows percentage of available space used) for both DBFS files and tables
# MAGIC   * Storage quota limit alert emails. Notifies customers via email when storage is exceeded or near licensed limits
# MAGIC
# MAGIC ## Analytic Functions
# MAGIC - Global Propensity
# MAGIC   * Optimization: ability leverage pre-built base tables for all available date partitions 
# MAGIC - Global Credit Scorecard
# MAGIC   * Option to append Supply Chain and Shipping features
# MAGIC   * Performance enhacements 
# MAGIC - Global Credit Limit Assignment CLA (updated scorecard same as above)
# MAGIC       
# MAGIC ## Visual Designer
# MAGIC - Global Propensity App
# MAGIC   * Optimization: ability leverage pre-built base tables for all available date partitions 
# MAGIC - Global Credit Scorecard App
# MAGIC   - Option to append Supply Chain and Shipping features
# MAGIC   - Usability enhancements
# MAGIC - Global Credit Limit Assignment App 
# MAGIC   - Option to append Supply Chain and Shipping features
# MAGIC   - Usability enhancements
# MAGIC - Batch Extract App
# MAGIC   - Option to export from databricks/Studion tables in addition to CSV files
# MAGIC              
# MAGIC
# MAGIC ## Core Data Releases
# MAGIC
# MAGIC | Table Name &nbsp;  | Use Case &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|--------------|--------------|------------------|
# MAGIC |dnb_intl_marketing_basetable|	MARKETING|	INTL| No | 	Curated marketing use case features for propensity and response model for international market |
# MAGIC |dnb_us_marketing_basetable|	MARKETING|	US| No | 	Curated marketing use case features for propensity and response model for USA |
# MAGIC
# MAGIC
# MAGIC ## Alternative Data Release
# MAGIC
# MAGIC | Bundle &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|
# MAGIC |intelligence360_saleswire| Sales trigger events for companies and government agencies throughout the USA.|