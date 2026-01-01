# Databricks notebook source
Release  : October 1, 2022

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400"> </img>
# MAGIC
# MAGIC ## Core Features
# MAGIC -
# MAGIC
# MAGIC ## Analytic Functions
# MAGIC - Global Propensity - Beta version
# MAGIC   * configuration: Ability to leverage both propensity and response through the same application.
# MAGIC - Global Credit Scorecard
# MAGIC   * Optimised data append process by incorporating risk base table.  
# MAGIC   * Market coverage - USA and INTL.  
# MAGIC   * Performance enhacements. 
# MAGIC - Global Credit Limit Assignment CLA (updated scorecard same as above)
# MAGIC       
# MAGIC ## Visual Designer
# MAGIC - Global Propensity App - Beta version
# MAGIC   * New application that leverages both propensity and response.
# MAGIC   * Enhanced user experience.
# MAGIC - Global Credit Scorecard App
# MAGIC   - Enhanced application to accomodate risk base table.
# MAGIC   - Usability enhancements.
# MAGIC - Global Credit Limit Assignment App 
# MAGIC   - Enhanced application to accomodate risk base table.
# MAGIC   - Usability enhancements.
# MAGIC          
# MAGIC
# MAGIC ## Core Data Releases
# MAGIC
# MAGIC | Table Name &nbsp;  | Use Case &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|--------------|--------------|------------------|
# MAGIC |dnb_intl_risk_basetable| RISK|	INTL| .| 	Features built exclusively for modeling international risk use cases . Monthly or Quarterly data available based on Studio Configuration.|
# MAGIC |dnb_us_risk_basetable|	RISK|	US|	.|	Features built exclusively for modeling US risk use cases . Monthly or Quarterly data available based on Studio Configuration.|
# MAGIC |dnb_activity_score_us|RISK|	US|	Pilot|	Commercial activity level/score table for US duns. |
# MAGIC |dnb_disruption_index_us|	RISK|	US|	Pilot|	This index provides a measure of the magnitude of the business disruption in the current week for US duns. Business Disruption can arise due several factors. It could be due company’s internal problem or due to problem in the industry in general or due to location problem. Some examples that can cause business disruption are natural calamity, severe macro-economic condition, bad company performance, shipment delays, delinquency rate in the industry in general or economic and trade sanctions etc. The Business Disruption Index will provide a measure of disruption by its location component, industry component, company health component as well as over disruption score.|
# MAGIC |dnb_disruption_index_intl|	RISK|	INTL|	Pilot|	This index provides a measure of the magnitude of the business disruption in the current week for INTL duns. Business Disruption can arise due several factors. It could be due company’s internal problem or due to problem in the industry in general or due to location problem. Some examples that can cause business disruption are natural calamity, severe macro-economic condition, bad company performance, shipment delays, delinquency rate in the industry in general or economic and trade sanctions etc. The Business Disruption Index will provide a measure of disruption by its location component, industry component, company health component as well as over disruption score.|