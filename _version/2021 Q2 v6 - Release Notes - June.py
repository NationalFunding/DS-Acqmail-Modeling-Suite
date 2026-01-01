# Databricks notebook source
Version  : v6 
Release  : June 25, 2021

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400">
# MAGIC
# MAGIC 1) *** Analytic Functions ***
# MAGIC
# MAGIC   - Credit Risk Scorecard v2 (RISK)
# MAGIC     - New Features: 
# MAGIC       - Custom binning for categorical and numerical features
# MAGIC       - Added model refit options
# MAGIC       - VIF and standard coefficients added to the model report
# MAGIC       - Improved spark-based scoring
# MAGIC    
# MAGIC     - Bug fixes: 
# MAGIC       - Fixed weighting option for Credit Risk Scorecard and AutoML 
# MAGIC
# MAGIC 2) *** Analytics Studio Visual Designer - v2 ***
# MAGIC
# MAGIC   - This release comes with these Visual Designer Apps:
# MAGIC       - AutoML App (MKT, RISK)
# MAGIC         - UI for Machine Learning Classification Model that allows user to build high performing model in an automated manner.
# MAGIC         - Selection between 2 Segments - US Risk & US Marketing.
# MAGIC         - Data Append based on the selection of load_year & load_month followed by Scoring.
# MAGIC       - Personalized Collections Prioritization App (USA) (RISK)
# MAGIC         - UI for combining aging information with the D&B predictive score to achieve actionable framework.
# MAGIC         - Option to choose between multiple modes - predefined collection risk segment, CCS/FSS scores or output from the Automated Credit Risk Scorecard model.
# MAGIC         - Reports to demonstrate summary table and different collections strategies and treatments.
# MAGIC       - DUNS Matching App 
# MAGIC         - UI for mapping csv file to a match structure 
# MAGIC         - integration with Analytics Studio Match functionality
# MAGIC       - UI and Performance Enhancements
# MAGIC         - Scorecard App 
# MAGIC         - Append function to capture Firmographics instead of Risk attributes table.
# MAGIC         - Application is built on Weighted Credit Risk Automated Scorecard instead of Automated Scorecard.
# MAGIC          
# MAGIC 3) *** Batch Model Automation ***
# MAGIC  
# MAGIC  - This release comes with the Batch Model Automation of Deployment:
# MAGIC       - Customer trained model is copied into the D&B Production environment 
# MAGIC       - Scoring process is operationalized within the deployment workflow  
# MAGIC       - Once the scoring completes, the results are shared via STP
# MAGIC        
# MAGIC 4) *** Data Bundle Releases: ***
# MAGIC
# MAGIC | Bundle &nbsp;  | Use Case &nbsp;| GEO &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|
# MAGIC |FSPS - Financial Solutions Prospect Suite	| MKT	| US | . |	 FSPS provides six scores that predict the likelihood of a business to engage in specific financial  transactions. The scores are derived from proprietary models and algorithms that look at the following activities.|
# MAGIC |Layoff Score |	RISK, MKT |	US	| . |	The Layoff Score predicts the likelihood that a business will experience a large layoff within the next 6 months.   A score specifically designed to identify large layoffs provided improved targeting of these businesses, with better sensitivity and fewer false positives.|
# MAGIC |ESG - Environmental, Social and Governance Indexes | RISK, MKT | US | Pilot | D&B ESG Indexes consists of a family of indexes of Environmental, Social and Governance.|
# MAGIC | Foot Traffic | RISK, MKT	| US | . | Foot Traffic Data|
# MAGIC | SBRI Sandbox Attributes | RISK | US | . | SBRI Sandbox Attributes|
# MAGIC
# MAGIC 5) *** Alternative Data Release ***
# MAGIC
# MAGIC | Bundle &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|
# MAGIC |construction_monitor_permits | Each week, Construction Monitor collects information on tens of thousands of residential, commercial, and solar construction projects nationwide that customers can use to identify leads, conduct risk assessment, conduct competitive analysis, and more. Real-time and historical information on building permits, swimming pool permits, and solar permits are included so that customers can react to emerging opportunities and analyze potential future projects. When combined with Dun & Bradstreet data, customers will have access to another key business activity signal, which can help identify key prospects with higher likelihood or propensity to need relevant goods and services. Construction Monitor data can also be combined with D&B Linkage data to create a view of construction activity across a full corporate family tree and potentially growth or retraction indicators.
# MAGIC |emolument_dataset | Emolument collects salary information from professionals across companies, industries and location via its website. In addition to salary & bonus information, each Emolument entry contains information on the job title, experience & educational background of the employee to whom the salary information belongs.|
# MAGIC |fraudfactors_similarity_index | The Similarity Index quantifies the textual differences between sections in a given company's annual filings on an "as disclosed" basis. For example, similarity scores are calculated by comparing the risk section within a company's 2017 10-K with the 2016 10-K. If substantial changes are identified (i.e., more risks added into the section), the similarity score will be low — indicating that a further investigation of what changed may be appropriate.|
# MAGIC |fundz_filing_data | Fundz data helps customers to find and connect with companies that have received venture capital funding by amount, industry, date or location. Data is collected from the U.S. Securities & Exchange Commission (SEC) and other public sources. Using the Dun & Bradstreet D-U-N-S® Number, Fundz data is integrated with Dun & Bradstreet's executive contacts and phone number for easier look up.|
# MAGIC |linkup_job_descriptions| This contains only the job descriptions. To use job descriptions, you will join with Job records on hash. For NLP applications you would typically join this with job descriptions and use a combination of job title from job records file and job description.|
# MAGIC |linkup_job_records|	This is the core of LinkUp data. Generally models are build off of this data, or aggregates of the data. In general when using any other file, you will want to do a left join with this file - dropping any reference information that does not have a job record (ie companies with no job records).|
# MAGIC |linkup_pit|	This shows point in time company information for company ids. This can be joined to job records to understand some corporate change (ie corporate name change, url changes, etc.). This would be joined to job records or aggregates to be used, dropping any company ids with no job records.|
# MAGIC |rsmetrics_metal_signals| RS Metrics' ESG Signals provides environmental impact and climate physical risk data and metrics for some of the most polluting and physical asset-intensive industries. The data and metrics are provided at the asset-level which allows customers to compare the risk profile of assets with similar attributes and production capacities. RS Metrics' ESG Signals data is combined with Dun & Bradstreet's identity, firmographic, and linkage data to further expand the view of the metals mining signals available in ESG Signals. Moreover, the combined data allows for an in-depth comparison for investing themes based on ESG integration, best-in-class, and sustainability themed investing. The ideal use case for the combined data is for gaining and comparing ESG insights. This combined data allows for increased efficiency compared to traditional ESG reporting methods.|
# MAGIC |sigwatch_targeted_corporates|	SIGWATCH activist targeting data provides a valuable, independent qualitative and quantitative assessment of corporate performance across ESG criteria, as well as early warning of emerging issues and problems derived directly from the statements of NGOs (non-governmental organizations) and activist groups. This data is obtained by tracking the campaigning communications and actions of 10,000 activist groups across the world, which are scored for levels of criticism and praise of over 20,000 corporate entities, projects, and brands, of which over 3,000 are listed corporations.|
# MAGIC |sigwatch_targeted_sectors|	SIGWATCH activist targeting data provides a valuable, independent qualitative and quantitative assessment of corporate performance across ESG criteria, as well as early warning of emerging issues and problems derived directly from the statements of NGOs (non-governmental organizations) and activist groups. This data is obtained by tracking the campaigning communications and actions of 10,000 activist groups across the world, which are scored for levels of criticism and praise of over 20,000 corporate entities, projects, and brands, of which over 3,000 are listed corporations.|
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC 6) *** Data Bundle Updates: ***
# MAGIC
# MAGIC - Removed a bundle and replaced it with production score
# MAGIC   - Removed bundle: dnb_foot_traffic (Pilot - RISK, MKT) [US] 
# MAGIC   - Replaced it with new bundle: dnb_foot_traffic_us (RISK, MKT) [US] 