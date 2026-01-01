# Databricks notebook source
Version  : v8
Release  : October 15, 2021

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/dam/english/image-library/Modernization/logos/logo-dnb-analytics-studio-color.png" width="400">
# MAGIC
# MAGIC 1) *** Analytic Functions ***
# MAGIC
# MAGIC   - Global Credit Scorecard (US, Global)(RISK)
# MAGIC     - New Features: 
# MAGIC       - Expanded the Credit Scorecard model into Global portfolio use case.
# MAGIC       - Added model segmentation into US and Intl markets with failure score optimization.
# MAGIC       - Provided ability to build models at the HQ/ Standalone level across globe.
# MAGIC       - Enhanced append process for US portfolio.
# MAGIC       
# MAGIC   - Global Credit Limit Assignment CLA (US, Global)(RISK)
# MAGIC     - New Features:
# MAGIC       - Expanded the Credit Limit Assignment model into Global portfolio use case.
# MAGIC       - Provided ability to build CLA with Scorecard or without Scorecard(using Flagship scores). 
# MAGIC       - Added append process for INTL portfolio. 
# MAGIC       - Added ability to choose the Risk Grouping.  
# MAGIC   
# MAGIC   - Propensity Model (MKT)
# MAGIC     - New Features:
# MAGIC       - Added addressable market filters for the input and output restrictions.
# MAGIC       - Added options for the scoring output selection of records.
# MAGIC       - Added filter based on Country for INTL and based on States for US.
# MAGIC       - Added Zip code inclusion and exclusion option for both US & INTL. 
# MAGIC       
# MAGIC   - AutoML (MKT, RISK)
# MAGIC     - New Features:
# MAGIC       - Training and deploying models for the following target definitions:
# MAGIC         - Binary Classification (Catboost/LightGBM/XGBoost)
# MAGIC         - Regression (LightGBM/XGBoost)
# MAGIC       - Added ability to choose one or more algorithms to run simultaneously.
# MAGIC       - Added flexibility to control model iterations for selected algorithms. 
# MAGIC       - Added flexibility to perform EDA and choose the final dataframe for model training.  
# MAGIC       - Added new reports to compare results of best models(with best hyperparameters) of each chosen algorithms.
# MAGIC       - Flexibility to choose the final model based on comparison reports of models. 
# MAGIC       
# MAGIC   - DEX Sampler Capabilities
# MAGIC     - The DEX Sampler Utility allows the customers to select an Alt. Data source from the Marketplace snowflake platform to get an adhoc sample dataset (with upto    100K records) selected based on a DUNS input or random selection logic. 
# MAGIC     - Features:
# MAGIC       - Wrapper Notebooks inclusive of the Customer Parameters and functions calling the API supporting the DEX Sampler Utility. 
# MAGIC       - Ability to choose DUNS based on the input file/input table given by the customer or a random selection of DUNS performed internally.
# MAGIC       - Ability to read the config file of the customer to extract the enablement/disablement for the utility.
# MAGIC   
# MAGIC   
# MAGIC   - Batch Extract Capability
# MAGIC     - This gives the capability for adhoc data delivery for the following set of licensed layouts available for selection by the customer, namely: DUNS only, TPA, TPA+IT, SDMR.
# MAGIC     - Features:
# MAGIC       - Wrapper Notebooks inclusive of the Customer Parameters and functions calling the API supporting the Batch Extract Utility. 
# MAGIC       - Ability to read the config file of the customer to extract the type of layout subscribed by the customer for the utility.
# MAGIC
# MAGIC 2) *** Analytics Studio Visual Designer - v3 ***
# MAGIC
# MAGIC   - This release comes with these Visual Designer Apps:
# MAGIC       - Global Credit Scorecard (USA, INTL, Global)(RISK)
# MAGIC         - Enhanced UI for the Credit Scorecard model to support Global portfolio.
# MAGIC         - Data Append (US, INTL) based on the risk optimized layouts with the selection by load_year & load_month.
# MAGIC         - Comparison against standard scores(example: fss points, ccs points for US and gbr_score for INTL)
# MAGIC         - Reports and charts to demonstrate summary table and model results.
# MAGIC         
# MAGIC       - Global Credit Limit Assignment (USA, INTL, Global) (RISK)
# MAGIC         - Enhanced UI for the Credit Limit Assignment model to support Global portfolio.
# MAGIC         - Data Append (US, INTL) based on the risk optimized layouts with the selection by load_year & load_month.        
# MAGIC         - Comparison against standard scores(example: fss points, ccs points for US and gbr_score for INTL)   
# MAGIC         - Ability to select different modes(CUSTOM, STANDARD) for US portfolio.
# MAGIC         - Ability to build the CLA model with/without Scorecard.
# MAGIC         - Reports and charts to demonstrate summary table and model results.
# MAGIC         
# MAGIC       - Propensity Model UI (USA) (MKT)
# MAGIC         - Option to upload a file with duns as a mandatory column or a table in data bricks.
# MAGIC         - Provide ability for Addressable Market Input restriction (Include and Exclude functionality for SICs and ZIPCODEs).
# MAGIC         - Provide ability for Addressable Market Output restriction (Include and Exclude functionality for SICs and ZIPCODEs).
# MAGIC         - Customer has the option to limit the random selection while training the data.
# MAGIC         - Model scores and reports are generated for customer reference.
# MAGIC    
# MAGIC       - DEX Sampler UI
# MAGIC         - UI for sourcing Alternative Data sources from the Marketplace to get an adhoc sample dataset (with upto 100K records) selected based on a DUNS input or random selection logic
# MAGIC         - Integration with Analytics Studio for loading data into user's workarea
# MAGIC         
# MAGIC       - Batch Extract
# MAGIC         - Ability to extract licensed data out of Analytics Studio environment based on following layouts:
# MAGIC           - DUNS Only
# MAGIC           - TPA
# MAGIC           - TPA + IT (Intelligent Targeting)
# MAGIC           - SDMR
# MAGIC          
# MAGIC 3) *** Batch Model Automation ***
# MAGIC  
# MAGIC  - This release comes with the Batch Model Automation of Deployment:
# MAGIC       - Customer trained model is copied into the D&B Production environment 
# MAGIC       - Scoring process is operationalized within the deployment workflow  
# MAGIC       - Once the scoring completes, the results are shared via STP
# MAGIC        
# MAGIC 4) *** Data Table Releases: ***
# MAGIC
# MAGIC | Table Name &nbsp;  | Use Case &nbsp;| Geoscope &nbsp; | Experimental  &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|
# MAGIC |dnb_borrowing_capacity_index_us|	RISK, MKT|	US| .| 	D&B Borrowing Capacity Index provides ability to quantiy risk level and assess small business borrowing capacity|
# MAGIC |dnb_global_ticker	|USER REF|	Global|	.| 	Global Ticker Symbols and Exchange information|
# MAGIC |dnb_vertical_attrib_communications|	RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC |dnb_vertical_attrib_finance|RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC |dnb_vertical_attrib_manufacturing|	RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.
# MAGIC |dnb_vertical_attrib_retail_trade|	RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC |dnb_vertical_attrib_services|	RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC |dnb_vertical_attrib_transportation|	RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC |dnb_vertical_attrib_utilities|	RISK|	US|	Pilot|	D&B vertical specific detailed trade attributes aggregated at the HQ DUNS level. D&B has created derived, actionable attributes from millions of transactional variables that can be used as building blocks for developing best-in-class predictive models.|
# MAGIC |dnb_vertical_risk_scores_communications|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.| 
# MAGIC |dnb_vertical_risk_scores_finance|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.|
# MAGIC |dnb_vertical_risk_scores_manufacturing|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.| 
# MAGIC |dnb_vertical_risk_scores_retail_trade|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.| 
# MAGIC |dnb_vertical_risk_scores_services|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.|
# MAGIC |dnb_vertical_risk_scores_transportation|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.| 
# MAGIC |dnb_vertical_risk_scores_utilities|	RISK|	US|	Pilot|	D&B vertical specific delinquency scores. D&B scores  predict whether the business with pay in delinquent manner on specific vertical of trade.| 
# MAGIC |shipment_container_status_event|	RISK, MKT|	Global|	. | Shipping data with information for a container event records the information regarding the various tracking status for the container.|
# MAGIC |shipment_counterparty|	RISK, MKT|	Global|	.| Shipping data with information for a Counter Party which can be a Shipper, Forwarder, Consignee or Notify Party. This view provides the counter part details with dnb_match_acceptance_indc ( D&B-derived attribute which notes whether or not the DUNS Number for this counterparty should be accepted.  Where 1 = accept, 0 == decline or review) is equal to 1.|
# MAGIC |shipment_counterparty_package|	RISK, MKT|	Global|	.|	Shipping data with shipment details. A Package is a small shipment, typically below 150 pounds. This view provides the package details with dnb_match_acceptance_indc ( D&B-derived attribute which notes whether or not the DUNS Number for this counterparty should be accepted.  Where 1 = accept, 0 == decline or review) is equal to 1.|
# MAGIC |shipment_counterparty_package_hscode|	RISK, MKT|	Global|	. |	Shipping data with HS codes. A Harmonized Detail will record the HS codes and related data as present in source data. The Harmonized  System is an internationally accepted system used to classify products. The first six digits of an HS code are universal across all countries, but each country will add additional digits to further specify products. HS codes play a role in determining import and export controls as well as duty rates. This view provides the Harmonized Code details of an package  with dnb_match_acceptance_indc ( D&B-derived attribute which notes whether or not the DUNS Number for this counterparty should be accepted.  Where 1 = accept, 0 == decline or review) is equal to 1.|
# MAGIC
# MAGIC
# MAGIC
# MAGIC 5) *** Alternative Data Release ***
# MAGIC
# MAGIC | Bundle &nbsp; | Description &nbsp; |
# MAGIC |-------|-------------|
# MAGIC |aberdeen_business_intelligence|Business Intelligence|
# MAGIC |aberdeen_technology_install|Technology Install|
# MAGIC |apatics_provider_alias_dba|This table contains data elements of Other Names(Aliases)/DBA for Medical Providers.|
# MAGIC |apatics_provider_drug_research_money_received|This table contains data elements of Drug Research Money Received for Medical Providers.|
# MAGIC |apatics_provider_hospital_affiliation|This table contains data elements of Hospital Affiliation for Medical Providers.|
# MAGIC |apatics_provider_master|This table contains data elements of Master Records for Medical Providers.|
# MAGIC |apatics_provider_other_address|This table contains data elements of Other Names(Aliases)/DBA for Medical Providers.|
# MAGIC |apatics_provider_other_phone_email_web|This table contains data elements of Other Phone, email, website and fax number for Medical Providers.|
# MAGIC |apatics_provider_sanction|This table contains data elements of Health/Federal Sanctions for Medical Providers.|
# MAGIC |bigbyteinsights_multi_family|The Multifamily dataset from Big Byte allows clients to analyze the new lease growth rates for multifamily apartments in various geographic markets. Data is sourced from the websites of various companies and contains the building location, unit size (studio, 1-bed, 2-bed, etc.), the asking lease rate, and in certain cases the term of the lease (where available). The raw data is processed using various statistical and machine learning techniques to provide a view into  housing / cost of living in the US. When this data is combined with Dun & Bradstreet commercial insight, customers can target potential customers in areas of growth or retraction - depending on their use case.|
# MAGIC |bigbyteinsights_self_storage|The data in Big Byte Insights' Self Storage dataset allows clients to see how rental rates for new Self Storage customers are changing on a year-over-year basis. The year-over-year comparison takes out the impact of seasonality, helping clients compare growth trends across various geographic markets. Data is sourced from the websites of the various companies. The raw data contains the facility location, unit size, unit attributes (climate controlled, outside drive-up, etc.), the web rate, the walk-in rate, and the promotion. Raw data is processed using various statistical and machine learning techniques to calculate Metropolitan Statistical Area (MSA) level aggregate figures for the various companies. In particular, pricing fluctuations can be leveraged as a leading population / business inflow / outflow indicator for an MSA, especially when used in conjunction with Dun & Bradstreet data and other D&B Data Exchange sources.|
# MAGIC |bigbyteinsights_single_family|The Single-Family dataset from Big Byte allows clients to analyze the new lease growth rates for single-family homes in various geographic markets. Data is sourced from the websites of various companies and contains the building location, unit size (studio, 1-bed, 2-bed, etc.), the asking lease rate, and in certain cases the term of the lease (where available). The raw data is processed using various statistical and machine learning techniques to provide a view into  housing / cost of living in the US. When this data is combined with Dun & Bradstreet commercial insight, customers can target potential customers in areas of growth or retraction - depending on their use case.|
# MAGIC |csrhub_esg_ratings|CSRHub offers a comprehensive global set of consensus environmental, social and governance (ESG) ratings and information. Founded in 2007, CSRHub's Big Data system measures the ESG impact that drives corporate and investor environment, social and governance decisions. When customers use CSRHub ESG ratings alongside Dun & Bradstreet risk data, they will have a view of risk from corporate, environmental, social and governance standpoints.|
# MAGIC |customweather_additional_air_descriptor|Air descriptor lookup code and description|
# MAGIC |customweather_barometric_tendency|Barometric tendency lookup code and description|
# MAGIC |customweather_historical_hourly|CustomWeather houses hourly weather data from weather stations around the globe to help companies analyze time-of-day dependent questions such as: 1) When can I expect the highest amount of foot traffic? 2) Will supply chain be disrupted this winter? Is my supply chain disrupted by a hurricane coming? 3) When should we open and close? When CustomWeather™s weather data is attached to the Dun & Bradstreet D-U-N-S® Number, customers can understand how weather might impact a business location. For example: D&B has a record for a company in Waltham, MA. D&B can attach CustomWeather™s weather data from Logan International Airport to the D-U-N-S Number using D&B™s latitude and longitude data. That combined data set can help customers assess the impacts of an impending storm, including whether that storm might create service outages based on historical precedent.|
# MAGIC |customweather_locations_duns|Contains D-U-N-S numbers closest to weather stations of CustomWeather with cutoff distance of 50 miles.|
# MAGIC |customweather_precipitation_descriptor|Precipitation descriptor lookup code and description|
# MAGIC |customweather_sky_conditions|Sky conditions lookup code and description|
# MAGIC |customweather_sky_descriptor|Sky descriptor lookup code and description|
# MAGIC |customweather_temperature_descriptor|Temperature descriptor lookup code and description|
# MAGIC |customweather_uv_index_descriptor|UV index descriptor lookup code and description|
# MAGIC |customweather_weather_type|The METAR weather type codes as reported by the weather station and their descriptions|
# MAGIC |gnsquotient_measureoutcomevalues|G&S Quotient provides a comprehensive ESG (Environmental, Social and Governance) dataset on US Public companies, with 76 metrics that are updated daily. The data is sourced from company regulatory filings (e.g., Securities & Exchange Commission EDGAR platform), voluntary filings (e.g., Global Reporting Initiative), and other public sources. It is used in ESG benchmarking by both corporates and investors, and in short-term and longer-term alpha discovery by investors. Using the Dun & Bradstreet D-U-N-S Number, clients can pull in additional information like firmographics, corporate hierarchy or intent.|
# MAGIC |iprstrategies_valuefactor|In IPR Value Factor's company-level patent values dataset, all values of all single patents owned by a company are aggregated to a corporate patent portfolio value. The single patent values are calculated via a sophisticated, machine-learning-based process trained with real patent values from past patent transactions. Primary patent data are obtained from the patent offices directly and cover 170 jurisdictions. Preprocessing and calculation are done in-house. The calculated values have a very high accuracy, as borne out by hundreds of use cases.  The data for each company that has (or has had) patents is matched with the company's Dun & Bradstreet D-U-N-S® Number. Single-company datasets are available, as well as complete flat files containing all companies with current or past patents worldwide, including their patent portfolio value history and their Innovation Indicating Key Figures history back to 2007-01 on a monthly basis.|
# MAGIC |iqvia_onekey_hco_matched|IQVIA OneKey data can be used to  identify Healthcare Organizations (HCO) in the USA. Information provided is granular, so that clients can understand HCOs in detail - like average patient stay or number of ICU beds.  IQVIA OneKey can also be used for data validation, analytics, segmentation, sales and marketing. By connecting IQVIA OneKey data to Dun & Bradstreet D-U-N-S® Numbers, customers can gain insight into business risk and opportunity at the site, family, and health care organization level.|
# MAGIC |iqvia_onekey_hcp_w_hco_matched|IQVIA OneKey data can be used to  identify Healthcare Professionals (HCP) affiliated with each Healthcare Organization in the USA.|
# MAGIC |paragonintel_jettrack_aircraft|Information about Aircrafts in JetTrack's dataset|
# MAGIC |paragonintel_jettrack_airport|Information about Airports in JetTrack's dataset|
# MAGIC |paragonintel_jettrack_company|Information about Companies in JetTrack's dataset|
# MAGIC |paragonintel_jettrack_flight|JetTrack's dataset includes every flight taken by every corporate jet, allowing users to see where executives are spending their time. By spotting unusual flight activity and specific flights to competitors, users can better understand corporate actions in real time. Data is sourced from public record, however we use algorithms to cleanse location data and a team of analysts verify the ownership of each jet in our dataset. When combined with the Dun & Bradstreet D-U-N-S® Numbers, users will have a more granular understanding of companies visited, like city, SIC code or even corporate hierarchy.|
# MAGIC |paragonintel_jettrack_model|Information about Aircraft Models in JetTrack's dataset|
# MAGIC |paragonintel_jettrack_ownership|Information about Aircraft Ownerships in JetTrack's dataset|
# MAGIC |pipecandy_consolidated_data|PipeCandy discovers and tracks over a million - and growing - direct to consumer (D2C) brands (that may also sell in brick-and-mortar locations) and eCommerce merchants so that companies can plan better GTM strategies, enrich internal databases, generate leads, track trends and benchmark success. Data is collected by crawling eCommerce storefronts, legal terms, public sources like Wikis, customer reviews and partner APIs. Brands and merchants have goods in apparel, pet care, home and garden, sporting, beauty, personal care, food and beverage and more. Dun & Bradstreet D-U-N-S Numbers are appended to all PipeCandy records, where possible, so that clients can get a detailed understanding of intent and purchases.|
# MAGIC |reveliolabs_gender_file|Gender file focuses on the count, inflow, and outflow of particular gender and ethnicity at client defined granularity levels.|
# MAGIC |reveliolabs_inflow_file|Contains data on employees joining Revelio-tracked companies|
# MAGIC |reveliolabs_long_file|Long files contain a wide range of statistics at client defined granularity levels.|
# MAGIC |reveliolabs_outflow_file|Contains data on employees departing Revelio-tracked companies|
# MAGIC |reveliolabs_posting_file|Posting files contain job postings data|
# MAGIC |reveliolabs_skill_file|The skill file focuses on the count, inflow, and outflow of employees with particular skills at client defined granularity levels. An employee is assumed to have possessed skills listed in the most recent profile historically as well. The default skill_file clusters.|

# COMMAND ----------

