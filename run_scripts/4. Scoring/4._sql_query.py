# Databricks notebook source
BASE_QUERY_TEMPLATE = """
SELECT DISTINCT af.duns
        ,af.business_name
        ,af.mail_addr1
        ,af.phy_addr1
        ,lpad(b.zip_code,5,'0') AS zip_code
        ,b.alph_terr_code
        ,b.sic2
        ,ah.ccs_points

FROM 
    us_marketing.dnb_core_id_geo_us af
    INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
    ON af.load_year = {lyr} AND af.load_month = {lmon}
        AND af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month

    LEFT JOIN nationalfunding_sf_share.load_as_marketable_sic_c1 marketable_sic_c1
    ON b.sic4_primary_cd = marketable_sic_c1.sic_4 AND marketable_sic_c1.RUN_DATE = '{run_date}'

    LEFT JOIN us_marketing.dnb_core_risk_scores_hq_us ah
    ON af.load_year = {lyr} AND af.load_month = {lmon}
        AND af.duns = ah.duns AND af.load_year = ah.load_year AND af.load_month = ah.load_month

    ANTI JOIN nationalfunding_sf_share.load_as_activated_leads_nf activated_nf
    ON b.duns = int(activated_nf.duns) AND activated_nf.RUN_DATE = '{run_date}'

    ANTI JOIN nationalfunding_sf_share.load_as_activated_leads_qb activated_qb
    ON b.duns = int(activated_qb.duns) AND activated_qb.RUN_DATE = '{run_date}'

WHERE 1=1
"""

ONLY_US_STATES_QUERY = """
-- Don't market to US territories
  AND af.phy_terr_code NOT IN (
    76, -- Puerto Rico
    92 --Virgin Islands
    )

  -- Don't market to US territories
  AND af.phy_terr_abv NOT IN (
    "", -- blanks
    "VI", --Virgin Islands
    "PR" -- Puerto Rico
    )

  -- Don't market to US territories
  AND af.phy_terr_name NOT IN (
    "", -- blanks
    "Virgin Islands",
    "Puerto Rico"
    )

  -- Must be in the USA
  AND af.iso_ctry_code = "US"
"""

NO_NULL_BUSINESS_QUERY = """
-- No blank, empty, nor null business names

  AND af.business_name NOT IN (
     "",
     "-"
    )

   AND af.business_name IS NOT NULL
"""

ACTIVE_BUSINESS_QUERY = """
--Business is still operating at this address
  AND af.active_ind = 1
"""

MAIN_LOCATION_QUERY = """
--Identifies whether the business is a single location entity, a headquarters, or a branch.
  --0 =  Single Location (no other businesses report to or are linked to the business);
  --1 = Headquarters or Parent (branches and or  subsidiaries report to the business);
  --2 = Branch (a secondary location to a headquarters)
  AND af.status_code in (0, 1)
"""

NO_EMPTY_ADDRESS_QUERY = """
AND (
    -- If set to Y, indicates that all physical address components (Street Address Line 1, City, and Postal Code) are populated and the deliverable address field is NOT set to Y.   
       af.avil_phy_addr = "Y"
    -- When set to Y  indicates that the mail address components Street Address Line 1  City and Postal Code.
    -- Only for Countries or Regions that require one is populated and the deliverable address is not set to Y. 
    OR af.avil_mail_addr = "Y"
  )
"""

C1_APPEND_QUERY = """
  -- Code indicating the nature of the record
  -- 0 = All Full reports and Branches with BOTH a valid SIC (non-9999) and telephone number.
  -- 1 = All Duns Support Records with BOTH a valid SIC (non-9999) and telephone number
  -- 2 = Any report type missing either a valid SIC or telephone number. 
  AND af.rec_clas_typ_code IN (1, 0)
"""
  
MARKETABILITY_C1_QUERY =  """
--Marketable records are a subset of the full D&B database and consist of the most probable connectivity for marketing type activities.
  -- Dispositions of this field are
  -- 0 = non-Marketable
  -- 1 = Marketable
  AND af.marketability_ind = 1 

  -- Marketable sic
  AND marketable_sic_c1.sic_4 IS NOT NULL
"""

C2_APPEND_QUERY = """
  -- Code indicating the nature of the record
  -- 0 = All Full reports and Branches with BOTH a valid SIC (non-9999) and telephone number.
  -- 1 = All Duns Support Records with BOTH a valid SIC (non-9999) and telephone number
  -- 2 = Any report type missing either a valid SIC or telephone number. 
  AND af.rec_clas_typ_code = 2
"""

QUERY_STR = {}
QUERY_STR[1] = BASE_QUERY_TEMPLATE + ONLY_US_STATES_QUERY + NO_NULL_BUSINESS_QUERY + ACTIVE_BUSINESS_QUERY + MAIN_LOCATION_QUERY + NO_EMPTY_ADDRESS_QUERY + C1_APPEND_QUERY + MARKETABILITY_C1_QUERY
QUERY_STR[2] = BASE_QUERY_TEMPLATE + ONLY_US_STATES_QUERY + NO_NULL_BUSINESS_QUERY + ACTIVE_BUSINESS_QUERY + MAIN_LOCATION_QUERY + NO_EMPTY_ADDRESS_QUERY + C2_APPEND_QUERY

# COMMAND ----------

ACTIVATED_LEAD_FOR_NF = """
ANTI JOIN nationalfunding_sf_share.load_as_activated_leads_nf activated_nf
    ON b.duns = int(activated_nf.duns) AND activated_nf.RUN_DATE = '{run_date}'
"""

ACTIVATED_LEAD_FOR_QB = """
ANTI JOIN nationalfunding_sf_share.load_as_activated_leads_qb activated_qb
    ON b.duns = int(activated_qb.duns) AND activated_qb.RUN_DATE = '{run_date}'

WHERE 1=1
"""

# COMMAND ----------

RETRAIN_C2_MARKETABLE_DATA_QUERY = """
SELECT DISTINCT af.duns
        ,af.business_name
        ,CASE WHEN marketable_sic_c1.sic_4 IS NOT NULL THEN true ELSE false END AS marketable

FROM  us_marketing.dnb_core_id_geo_us af
INNER JOIN us_marketing.dnb_bus_dimensions_us ag
    ON af.duns = ag.duns AND af.load_year = ag.load_year AND af.load_month = ag.load_month
    AND af.rec_clas_typ_code IN (1, 0) 
    AND af.marketability_ind = 1 
    AND af.business_name IS NOT NULL
    AND ag.sic4 IS NOT NULL

    AND make_date(af.load_year, af.load_month, 1) < to_date('{lyr}-{lmon}-01')
    AND make_date(af.load_year, af.load_month, 1) >= add_months(to_date('{lyr}-{lmon}-01'), -3)
LEFT JOIN nationalfunding_sf_share.load_as_marketable_sic_c1 marketable_sic_c1
    ON ag.sic4 = marketable_sic_c1.sic_4 AND marketable_sic_c1.RUN_DATE = '{run_date}'
"""

# COMMAND ----------

CUSTOM_SCORE_CHECK_QUERY = """
SELECT COUNT(DISTINCT custom_response) as distinct_cnt_resp
, COUNT(DISTINCT custom_funded) as distinct_cnt_funded
FROM nf_workarea.model_data_input"""

# COMMAND ----------

MODEL_DATA_QUERY_V7 = """
with ucc as 
(
select distinct load_year, load_month, filing_duns as duns, max(filing_date) as ucc_filing_date
from us_ucc_filing_data.dnb_ucc_filings
group by 1, 2, 3
)
SELECT DISTINCT af.duns
,mail.nf_mailable
,mail.qb_mailable
,af.bus_strt_yr
,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN c1_resp.score ELSE c2_resp.score END AS dnb_custom_response
,mail_history_nf.last_mail_date as nf_last_mail_date
,mail_history_nf.first_mail_date as nf_first_mail_date
,mail_history_nf.num_times_mailed as nf_num_times_mailed
,mail_history_qb.last_mail_date as qb_last_mail_date
,mail_history_qb.first_mail_date as qb_first_mail_date
,mail_history_qb.num_times_mailed as qb_num_times_mailed
,mail_history_nf.cadence_pattern as nf_cadence_pattern
,mail_history_qb.cadence_pattern  as qb_cadence_pattern
,aa.advertising_buydex
,aa.buydex
,aa.less_than_truckload_buydex
,aa.it_buydex
,aa.food_bev_buydex
,ah.ccs_points
,ah.tlp_score
,ah.via_data_depth
,ah.via_profile
,an.d_sat_hq
,aq.inq_sic61_inq_12m
,b.alph_terr_code
,b.gc_employees
,b.gc_sales
,b.inq_inquiry_duns_12m
,b.inq_inquiry_duns_24m
,b.inq_inquiry_duns_6m
,b.nbr_ucc_filings
,b.regular_inq_cnt
,b.sic2
,b.sic4
,bahq.ba_cnt_cr_12m
,bahq.ba_sum_z_sic_12m
,bahq.ba_sum_b_6m
,bahq.ba_sum_f_mtch8_12m
,los.layoff_score_state_percentile

,afc.chg_cosmetic_name_change_3m
,afc.chg_location_ind_3m
,afc.chg_operates_from_residence_ind_3m
,afc.chg_owns_ind_3m
,afc.chg_status_code_3m
,afc.chg_avil_phy_addr_3m
,afc.chg_rec_clas_typ_code_3m
,afc.chg_small_business_ind_3m
,msc.chg_buydex_n10pct_3m
,msc.chg_mc_borrowing_growth_to_stable_3m
,msc.chg_composite_risk_3m
,msc.chg_triple_play_segment_3m


, CASE WHEN ppp.EXTERNAL_ID_MARKETING IS NOT NULL THEN 1 ELSE 0 END AS flg_ppp
, CASE WHEN ppp.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp
,an.totdoll_hq
,ar.dt_12m_hi_cr_mo_av_amt
,b.dolcur
,afc.chg_manufacturing_ind_1m
,CASE 
        WHEN ddu.phy_bus_loc_code IS NULL THEN NULL
        WHEN ddu.phy_bus_loc_code IN ('A', 'B', 'C', 'D') THEN 1
        ELSE 0
    END AS rdi_indicator
,ag.non_fin_trade_balance

FROM  us_marketing.dnb_core_id_geo_us af
INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
ON af.load_year = {lyr} AND af.load_month = {lmon} 
    AND af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month

INNER JOIN nf_workarea.mailable_pop mail ON mail.duns = b.duns


LEFT JOIN nationalfunding_sf_share.load_as_mail_history_nf mail_history_nf ON af.duns = int(mail_history_nf.duns) AND mail_history_nf.RUN_DATE = '{run_date}'
LEFT JOIN nationalfunding_sf_share.load_as_mail_history_qb mail_history_qb ON af.duns = int(mail_history_qb.duns) AND mail_history_qb.RUN_DATE = '{run_date}'

LEFT JOIN us_marketing.dnb_bus_dimensions_us ag
ON b.duns = ag.duns AND b.load_year = ag.load_year AND b.load_month = ag.load_month

LEFT JOIN ucc 
ON af.duns=ucc.duns and af.load_year=ucc.load_year and af.load_month=ucc.load_month

LEFT JOIN  us_marketing.dnb_dsf_us ddu 
ON ddu.duns = b.duns AND b.load_year = ddu.load_year AND b.load_month = ddu.load_month

LEFT JOIN us_marketing.dnb_core_marketing_scores_us aa
ON b.duns = aa.duns AND {lyr_prior} = aa.load_year AND {lmon_prior} = aa.load_month

LEFT JOIN us_marketing_features.dnb_id_geo_chg_us afc 
ON b.duns = afc.duns AND b.load_year = afc.load_year AND b.load_month = afc.load_month

LEFT JOIN us_marketing.dnb_core_risk_scores_hq_us ah
ON b.duns = ah.duns AND b.load_year = ah.load_year AND b.load_month = ah.load_month

LEFT JOIN us_marketing.dnb_core_legacy_payment_attrib_hq_us an
ON b.duns = an.duns AND b.load_year = an.load_year AND b.load_month = an.load_month

LEFT JOIN us_marketing.dnb_da_business_inquiries_hq_us aq
ON b.duns = aq.duns AND b.load_year = aq.load_year AND b.load_month = aq.load_month

LEFT JOIN us_marketing.dnb_da_business_activity_hq_us bahq
ON b.duns = bahq.duns AND b.load_year = bahq.load_year AND b.load_month = bahq.load_month

LEFT JOIN us_marketing.dnb_layoff_score_us los
ON b.duns = los.duns AND b.load_year = los.load_year AND b.load_month = los.load_month

LEFT JOIN us_marketing_features.dnb_marketing_scores_chg_us msc 
ON b.duns = msc.duns AND {lyr_prior} = msc.load_year AND {lmon_prior} = msc.load_month

LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp
ON ppp.EXTERNAL_ID_MARKETING=b.duns

LEFT JOIN us_marketing.dnb_da_dtri_hq_us ar 
ON b.duns = ar.duns AND b.load_year = ar.load_year AND b.load_month = ar.load_month

LEFT JOIN nf_workarea.scored_c1_response_{lyr}{lmon} c1_resp ON af.duns=c1_resp.duns
LEFT JOIN nf_workarea.scored_c2_response_{lyr}{lmon} c2_resp ON af.duns=c2_resp.duns

"""

# COMMAND ----------

MODEL_DATA_QUERY_V6 = """
with ucc as 
(
select distinct load_year, load_month, filing_duns as duns, max(filing_date) as ucc_filing_date
from us_ucc_filing_data.dnb_ucc_filings
group by 1, 2, 3
)
SELECT DISTINCT af.duns
,mail.nf_mailable
,mail.qb_mailable
,af.bus_strt_yr
,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
,COALESCE(dnb_score_c1.RESPOND, dnb_score_c2.Response_Score) AS custom_response
,COALESCE(dnb_score_c1.FUND, dnb_score_c2.Fund_Score) as custom_funded
,mail_history_nf.last_mail_date as nf_last_mail_date
,mail_history_nf.first_mail_date as nf_first_mail_date
,mail_history_nf.num_times_mailed as nf_num_times_mailed
,mail_history_nf.cadence_pattern as nf_cadence_pattern
,mail_history_qb.last_mail_date as qb_last_mail_date
,mail_history_qb.first_mail_date as qb_first_mail_date
,mail_history_qb.num_times_mailed as qb_num_times_mailed
,mail_history_qb.cadence_pattern  as qb_cadence_pattern
,ucc.ucc_filing_date

,aa.advertising_buydex
,aa.buydex
,aa.less_than_truckload_buydex
,ah.ccs_points
,ah.tlp_score
,ah.via_data_depth
,ah.via_profile
,an.d_sat_hq
,aq.inq_sic61_inq_12m
,b.alph_terr_code
,b.ba_bd_highactivity_12m
,b.gc_employees
,b.gc_sales
,b.inq_inquiry_duns_12m
,b.inq_inquiry_duns_24m
,b.inq_inquiry_duns_6m
,b.nbr_ucc_filings
,b.regular_inq_cnt
,b.sic2
,b.sic4
,bahq.ba_cnt_cr_12m
,bahq.ba_cnt_cr_3m
,ddls.drp_ccs9_prctl_loc_index
,ddls.drp_ccs9_prctl_prf_index
,ddls.drp_fspct7p1_prf_index
,los.layoff_score_state_percentile

,afc.chg_cosmetic_name_change_3m
,afc.chg_location_ind_3m
,afc.chg_operates_from_residence_ind_3m
,afc.chg_owns_ind_3m
,afc.chg_status_code_3m
,afc.chg_avil_phy_addr_3m
,afc.chg_rec_clas_typ_code_3m
,afc.chg_small_business_ind_3m
,msc.chg_buydex_n10pct_3m
,msc.chg_mc_borrowing_growth_to_stable_3m
,msc.chg_composite_risk_3m
,msc.chg_triple_play_segment_3m

,an.pydx_1
,aj.comptype_hq
,0 as lease_propensity_segment
,0 as lineofcredit_propensity_segment
,0 as loan_propensity_segment
,0 as total_balance_segment
,0 as lease_balance_segment

,to_date(ppp.DateApproved, 'MM/dd/yyyy') as DateApproved
,cast(ppp.SBAOfficeCode as string) as SBAOfficeCode
,ppp.ProcessingMethod
,ppp.BorrowerName
,ppp.BorrowerAddress
,ppp.BorrowerCity
,ppp.BorrowerState
,ppp.BorrowerZip
,to_date(ppp.LoanStatusDate, 'MM/dd/yyyy') as LoanStatusDate
,ppp.LoanStatus
,ppp.Term
,ppp.SBAGuarantyPercentage
,ppp.InitialApprovalAmount
,ppp.CurrentApprovalAmount
,ppp.UndisbursedAmount
,ppp.FranchiseName
,cast(ppp.ServicingLenderLocationID as string) as ServicingLenderLocationID
,ppp.ServicingLenderName
,ppp.ServicingLenderAddress
,ppp.ServicingLenderCity
,ppp.ServicingLenderState
,ppp.ServicingLenderZip
,ppp.RuralUrbanIndicator
,ppp.HubzoneIndicator
,ppp.LMIIndicator
,ppp.BusinessAgeDescription
,ppp.ProjectCity
,ppp.ProjectCountyName
,ppp.ProjectState
,ppp.ProjectZip
,ppp.CD
,ppp.JobsReported
,cast(ppp.NAICSCode as string) as NAICSCode
,ppp.Race
,ppp.Ethnicity
,ppp.UTILITIES_PROCEED
,ppp.PAYROLL_PROCEED
,ppp.MORTGAGE_INTEREST_PROCEED
,ppp.RENT_PROCEED
,ppp.REFINANCE_EIDL_PROCEED
,ppp.HEALTH_CARE_PROCEED
,ppp.DEBT_INTEREST_PROCEED
,ppp.BusinessType
,cast(ppp.OriginatingLenderLocationID as string) as OriginatingLenderLocationID
,ppp.OriginatingLender
,ppp.OriginatingLenderCity
,ppp.OriginatingLenderState
,ppp.Gender
,ppp.Veteran
,ppp.NonProfit
,ppp.ForgivenessAmount
,to_date(ppp.ForgivenessDate,'MM/dd/yyyy') as ForgivenessDate
, CASE WHEN ppp.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp

,an.pydx_19
,an.totdoll_hq
,ar.dt_12m_hi_cr_mo_av_amt
,b.dolcur
,bahq.ba_sum_f_mtch8_12m
,afc.chg_manufacturing_ind_3m

FROM  us_marketing.dnb_core_id_geo_us af
INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
ON af.load_year = {lyr} AND af.load_month = {lmon} 
    AND af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month

INNER JOIN {db}.mailable_pop mail ON mail.duns = b.duns

LEFT JOIN nf_workarea.{dnb_score_table_name_c1} dnb_score_c1 ON af.duns = dnb_score_c1.duns
LEFT JOIN nf_workarea.{dnb_score_table_name_c2} dnb_score_c2 ON af.duns = dnb_score_c2.duns
LEFT JOIN nationalfunding_sf_share.load_as_mail_history_nf mail_history_nf ON af.duns = int(mail_history_nf.duns) AND mail_history_nf.RUN_DATE = '{run_date}'
LEFT JOIN nationalfunding_sf_share.load_as_mail_history_qb mail_history_qb ON af.duns = int(mail_history_qb.duns) AND mail_history_qb.RUN_DATE = '{run_date}'

LEFT JOIN ucc 
ON af.duns=ucc.duns and af.load_year=ucc.load_year and af.load_month=ucc.load_month

LEFT JOIN us_marketing.dnb_core_bus_firm_hq_us aj
ON b.duns = aj.duns AND b.load_year = aj.load_year AND b.load_month = aj.load_month

LEFT JOIN us_marketing.dnb_core_marketing_scores_us aa
ON b.duns = aa.duns AND {lyr_prior} = aa.load_year AND {lmon_prior} = aa.load_month

LEFT JOIN us_marketing_features.dnb_id_geo_chg_us afc 
ON b.duns = afc.duns AND b.load_year = afc.load_year AND b.load_month = afc.load_month

LEFT JOIN us_marketing.dnb_core_risk_scores_hq_us ah
ON b.duns = ah.duns AND b.load_year = ah.load_year AND b.load_month = ah.load_month

LEFT JOIN us_marketing.dnb_core_legacy_payment_attrib_hq_us an
ON b.duns = an.duns AND b.load_year = an.load_year AND b.load_month = an.load_month

LEFT JOIN us_marketing.dnb_da_business_inquiries_hq_us aq
ON b.duns = aq.duns AND b.load_year = aq.load_year AND b.load_month = aq.load_month

LEFT JOIN us_marketing.dnb_da_business_activity_hq_us bahq
ON b.duns = bahq.duns AND b.load_year = bahq.load_year AND b.load_month = bahq.load_month

LEFT JOIN us_marketing.dnb_da_location_site_us ddls ON b.duns = ddls.duns AND {lyr_qtr} = ddls.load_year AND {lmon_qtr} = ddls.load_month

LEFT JOIN us_marketing.dnb_layoff_score_us los
ON b.duns = los.duns AND b.load_year = los.load_year AND b.load_month = los.load_month

LEFT JOIN us_marketing_features.dnb_marketing_scores_chg_us msc 
ON b.duns = msc.duns AND {lyr_prior} = msc.load_year AND {lmon_prior} = msc.load_month

LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp
ON ppp.EXTERNAL_ID_MARKETING=b.duns

LEFT JOIN us_marketing.dnb_da_dtri_hq_us ar 
ON b.duns = ar.duns AND b.load_year = ar.load_year AND b.load_month = ar.load_month
"""

# COMMAND ----------

MODEL_DATA_QUERY_V50 = """
WITH ucc AS (
    SELECT DISTINCT mailable.duns, MAX(ucc.ucc_filing_date) ucc_filing_date
    FROM nf_workarea.nf_ucc ucc
        INNER JOIN {db}.mailable_pop mailable
        ON ucc.duns = mailable.duns AND cast(ucc.ucc_filing_date as numeric) <= ({lyr}*10000 + {lmon}*100 + 1)
    GROUP BY mailable.duns
)
SELECT DISTINCT af.duns
,mail.nf_mailable
,mail.qb_mailable
,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
,COALESCE(dnb_score_c1.RESPOND, dnb_score_c2.Response_Score) AS custom_response
,COALESCE(dnb_score_c1.FUND, dnb_score_c2.Fund_Score) as custom_funded
,mail_history_nf.last_mail_date as nf_last_mail_date
,mail_history_nf.first_mail_date as nf_first_mail_date
,mail_history_nf.num_times_mailed as nf_num_times_mailed
,mail_history_nf.cadence_pattern as nf_cadence_pattern
,mail_history_qb.last_mail_date as qb_last_mail_date
,mail_history_qb.first_mail_date as qb_first_mail_date
,mail_history_qb.num_times_mailed as qb_num_times_mailed
,mail_history_qb.cadence_pattern  as qb_cadence_pattern
,ucc.ucc_filing_date

,aa.advertising_buydex
,aa.buydex
,aa.less_than_truckload_buydex
,af.bus_strt_yr
,ah.ccs_points
,ah.tlp_score
,ah.via_data_depth
,ah.via_profile
,an.d_sat_hq
,an.pydx_19
,an.totdoll_hq
,aq.inq_sic61_inq_12m
,ar.dt_12m_hi_cr_mo_av_amt
,b.alph_terr_code
,b.dolcur
,b.gc_employees
,b.gc_sales
,b.inq_inquiry_duns_12m
,b.inq_inquiry_duns_24m
,b.inq_inquiry_duns_6m
,b.nbr_ucc_filings
,b.regular_inq_cnt
,b.sic4
,bahq.ba_cnt_cr_12m
,bahq.ba_cnt_cr_3m
,bahq.ba_sum_f_mtch8_12m
,los.layoff_score_state_percentile

,afc.chg_cosmetic_name_change_3m
,afc.chg_location_ind_3m
,afc.chg_manufacturing_ind_3m
,afc.chg_operates_from_residence_ind_3m
,afc.chg_owns_ind_3m
,afc.chg_status_code_3m
,afc.chg_avil_phy_addr_3m
,afc.chg_rec_clas_typ_code_3m
,afc.chg_small_business_ind_3m
,msc.chg_buydex_n10pct_3m
,msc.chg_mc_borrowing_growth_to_stable_3m
,msc.chg_composite_risk_3m
,msc.chg_triple_play_segment_3m

,an.pydx_1
,aj.comptype_hq
,0 as lease_propensity_segment
,0 as lineofcredit_propensity_segment
,0 as loan_propensity_segment
,0 as total_balance_segment
,0 as lease_balance_segment

, CASE WHEN ppp_df.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp

FROM  us_marketing.dnb_core_id_geo_us af
INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
ON af.load_year = {lyr} AND af.load_month = {lmon} 
    AND af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month

INNER JOIN {db}.mailable_pop mail ON mail.duns = b.duns

LEFT JOIN nf_workarea.{dnb_score_table_name_c1} dnb_score_c1 ON af.duns = dnb_score_c1.duns
LEFT JOIN nf_workarea.{dnb_score_table_name_c2} dnb_score_c2 ON af.duns = dnb_score_c2.duns
LEFT JOIN {db}.load_as_mail_history_nf mail_history_nf ON af.duns = mail_history_nf.duns
LEFT JOIN {db}.load_as_mail_history_qb mail_history_qb ON af.duns = mail_history_qb.duns
LEFT JOIN ucc ON af.duns=ucc.duns

LEFT JOIN us_marketing.dnb_core_bus_firm_hq_us aj
ON b.duns = aj.duns AND b.load_year = aj.load_year AND b.load_month = aj.load_month

LEFT JOIN us_marketing.dnb_core_legacy_payment_attrib_hq_us an
ON b.duns = an.duns AND b.load_year = an.load_year AND b.load_month = an.load_month

LEFT JOIN us_marketing.dnb_core_marketing_scores_us aa
ON b.duns = aa.duns AND {lyr_prior} = aa.load_year AND {lmon_prior} = aa.load_month

LEFT JOIN us_marketing.dnb_core_risk_scores_hq_us ah
ON b.duns = ah.duns AND b.load_year = ah.load_year AND b.load_month = ah.load_month

LEFT JOIN us_marketing.dnb_da_business_inquiries_hq_us aq
ON b.duns = aq.duns AND b.load_year = aq.load_year AND b.load_month = aq.load_month

LEFT JOIN us_marketing.dnb_da_dtri_hq_us ar 
ON b.duns = ar.duns AND b.load_year = ar.load_year AND b.load_month = ar.load_month

LEFT JOIN us_marketing.dnb_da_business_activity_hq_us bahq
ON b.duns = bahq.duns AND b.load_year = bahq.load_year AND b.load_month = bahq.load_month

LEFT JOIN us_marketing.dnb_layoff_score_us los
ON b.duns = los.duns AND b.load_year = los.load_year AND b.load_month = los.load_month

LEFT JOIN us_marketing_features.dnb_id_geo_chg_us afc 
ON b.duns = afc.duns AND b.load_year = afc.load_year AND b.load_month = afc.load_month

LEFT JOIN us_marketing_features.dnb_marketing_scores_chg_us msc 
ON b.duns = msc.duns AND {lyr_prior} = msc.load_year AND {lmon_prior} = msc.load_month

LEFT JOIN nf_workarea.matched_companies_02132025_extra_marketing_columns_parsed ppp_df
ON ppp_df.EXTERNAL_ID_MARKETING=b.duns
"""


# COMMAND ----------

READY_TO_BE_LICENSED_QUERY = """
SELECT
    picked.duns
  ,	model_input.class_type
  ,	model_input.gc_employees
  ,	model_input.pydx_1 AS pydx_1
  ,	model_input.gc_sales
  ,	model_input.ucc_filing_date
  ,	model_input.bus_strt_yr
  ,	model_input.comptype_hq AS comptype_hq
  ,	model_input.ccs_points
  ,	model_input.loan_propensity_segment AS loan_propensity_segment
  ,	model_input.lease_propensity_segment AS lease_propensity_segment
  ,	model_input.lineofcredit_propensity_segment AS lineofcredit_propensity_segment
  ,	model_input.total_balance_segment AS total_balance_segment
  ,	model_input.lease_balance_segment AS lease_balance_segment
  ,	CAST(null as double) AS creditcard_response_segment
  ,	model_input.custom_response
  ,	model_input.custom_funded

  ,CASE WHEN model_input.chg_cosmetic_name_change_3m=1
          or model_input.chg_location_ind_3m=1
          or model_input.chg_operates_from_residence_ind_3m=1
          or model_input.chg_owns_ind_3m=1
          or model_input.chg_status_code_3m=1
          or model_input.chg_avil_phy_addr_3m=1
          or model_input.chg_rec_clas_typ_code_3m=1
          or model_input.chg_small_business_ind_3m=1 THEN 'Y' ELSE 'N' END    AS entity_change

  ,CASE WHEN (model_input.class_type='c1' and (model_input.chg_buydex_n10pct_3m=1 or model_input.chg_mc_borrowing_growth_to_stable_3m=1))
              OR
              (model_input.class_type='c2' and (model_input.chg_composite_risk_3m=1 or model_input.chg_triple_play_segment_3m=1))
              THEN 'Y' ELSE 'N' END                                           AS material_change

  ,	picked.nf_proba_RESP AS nf_proba_1
  ,	picked.nf_proba_SUB AS nf_proba_2
  ,	0 AS nf_proba_3
  ,	0 AS nf_proba_4
  ,	picked.qb_proba_RESP AS qb_proba_1
  ,	picked.qb_proba_SUB AS qb_proba_2
  ,	0 AS qb_proba_3
  ,	0 AS qb_proba_4
  ,	CASE WHEN picked.nf_selected = 1 THEN CONCAT(score_current.nf_nlpl_flg
                                                      ,'-' , score_current.ste_seg,'-', ROUND(CAST(nf_rate_resp AS decimal(20,10)), 8)
                                                      ,'-', ROUND(CAST(nf_rate_qual AS decimal(20, 10)), 8)
                                                      ,'-', ROUND(CAST(nf_rate_sub AS decimal(20,10)), 8)
                                                      ,'-', ROUND(nf_rate_appr, 8)
                                                      ,'-', ROUND(nf_rate_fund, 8)
                                                      ,'-', ROUND(nf_fund_amt_per_deal, 8)
                                                      ,'-', ROUND(CAST(ctf_nf AS decimal(20,10)), 8), '-', model_input.ppp, '-', right(picked.nf_scorebin_RESP,2), '-', right(picked.nf_scorebin_SUB,2),'-', nf_mdl_name_RESP, '-',nf_mdl_name_SUB) END AS nf_market_segment
  ,	CASE WHEN picked.nf_selected = 1 THEN CONCAT(COALESCE(picked.dbl_select_grp, picked.testing_group), '/', picked.version) END AS nf_list_name
  ,	CASE WHEN picked.qb_selected = 1 THEN CONCAT(score_current.qb_nlpl_flg
                                                      ,'-' , score_current.ste_seg,'-', ROUND(CAST(qb_rate_resp AS decimal(20,10)), 8)
                                                      ,'-', ROUND(CAST(qb_rate_qual AS decimal(20, 10)), 8)
                                                      ,'-', ROUND(CAST(qb_rate_sub AS decimal(20,10)), 8)
                                                      ,'-', ROUND(qb_rate_appr, 8)
                                                      ,'-', ROUND(qb_rate_fund, 8)
                                                      ,'-', ROUND(qb_fund_amt_per_deal, 8)
                                                      ,'-', ROUND(CAST(ctf_qb AS decimal(20,10)), 8), '-', model_input.ppp, '-', right(picked.qb_scorebin_RESP,2), '-', right(picked.qb_scorebin_SUB,2),'-', qb_mdl_name_RESP, '-', qb_mdl_name_SUB)END AS qb_market_segment
  ,	CASE WHEN picked.qb_selected = 1 THEN CONCAT(COALESCE(picked.dbl_select_grp, picked.testing_group), '/', picked.version) END AS qb_list_name
  ,	CASE WHEN picked.sbl_group is not null THEN CONCAT(score_current.nf_nlpl_flg
                                                      ,'-' , score_current.ste_seg,'-', ROUND(CAST(nf_rate_resp AS decimal(20,10)), 8)
                                                      ,'-', ROUND(CAST(nf_rate_qual AS decimal(20, 10)), 8)
                                                      ,'-', ROUND(CAST(nf_rate_sub AS decimal(20,10)), 8)
                                                      ,'-', ROUND(nf_rate_appr, 8)
                                                      ,'-', ROUND(nf_rate_fund, 8)
                                                      ,'-', ROUND(nf_fund_amt_per_deal, 8)
                                                      ,'-', ROUND(CAST(ctf_nf AS decimal(20,10)), 8), '-', model_input.ppp, '-', right(picked.nf_scorebin_RESP,2), '-', right(picked.nf_scorebin_SUB,2),'-', nf_mdl_name_RESP, '-', nf_mdl_name_SUB) END AS sbl_market_segment
  ,	CASE WHEN picked.sbl_group is not null THEN CONCAT(picked.sbl_group, '/', picked.version) END AS sbl_list_name

  FROM {db}.final_selection_updated_complete picked
  INNER JOIN {db}.model_data_input_{main_version} model_input ON picked.duns = model_input.duns
  INNER JOIN {db}.AMP_score_current_{main_version} score_current on picked.duns=score_current.duns
"""

# COMMAND ----------

INSERT_INTO_ACQUISITIONMAILHISTORY_QUERY = """
INSERT INTO nf_workarea.acquisitionmailhistory_{brand}
SELECT DISTINCT ACQUISITIONMAILHISTORYKEY,
DUNSNUMBER,
BRAND,
BRANDCODE,
MAILRUNMONTH,
MAILRUNMONTHVAR,
FIRSTMAILMONTH,
FIRSTMAILMONTHVAR,
LASTMAILMONTH,
LASTMAILMONTHVAR,
TOTALTIMESMAILED,
CADENCEMONTHS,
CADENCEPATTERN,
CREATEDBY,
CREATEDDATE,
UPDATEDBY,
UPDATEDDATE,
DUNS,
MAIL_DATE,
RUN_DATE,
CAMPAIGNID,
ACCOUNTNUM,
LIST,
CUSTOM_RESPONSE,
CUSTOM_FUNDED,
REF_SCORE1,
REF_SCORE2,
REF_SCORE3,
REF_SCORE4,
MODEL_VERSION1,
MODEL_VERSION2,
MODEL_VERSION3,
MODEL_VERSION4,
FILE_TYPE,
RDI_INDICATOR,
NULL AS DROP_DATE
FROM nationalfunding_sf_share.load_as_mail_history_2_months_ago_{brand} WHERE MAIL_DATE = '{mail_date}'
"""

# COMMAND ----------

CTF_CALIB_TAB_QUERY = """
WITH dat as
(
    SELECT COALESCE(nf.duns, qb.duns) AS duns
    , nf.campaignid as nf_campaignid
    , qb.campaignid as qb_campaignid
    , nf.accountnum as nf_accountnum
    , qb.accountnum as qb_accountnum
    , ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -3) AS load_date
    , ADD_MONTHS(COALESCE(nf.mail_date, qb.mail_date), -2) AS run_date
    , COALESCE(nf.mail_date, qb.mail_date) AS mail_date
    , CASE WHEN nf.duns IS NOT NULL THEN CAST('true' AS BOOLEAN) END AS nf_mailable
    , CASE WHEN qb.duns IS NOT NULL THEN CAST('true' AS BOOLEAN) END AS qb_mailable
    FROM (
        (
            SELECT *
            FROM {db}.AcquisitionMailHistory_NF
            WHERE mail_date >= '{calib_start_dt}' AND mail_date <= '{calib_end_dt}' and list != 'Prescreen Remail/V6'
        ) nf
        FULL OUTER JOIN 
        (
            SELECT *
            FROM {db}.AcquisitionMailHistory_QB
            WHERE mail_date >= '{calib_start_dt}' AND mail_date <= '{calib_end_dt}' and (list != 'Prescreen Remail/V6' or list is null)
        )qb
        ON nf.duns = qb.duns AND nf.run_date = qb.run_date
    )
)
SELECT dat.*
    ,CASE WHEN af.rec_clas_typ_code IN (1, 0) THEN 'c1' else 'c2' END AS class_type
    , nf_resp.proba AS nf_proba_RESP
    , nf_resp.mdl_name AS nf_mdl_name_RESP
    , nf_sub.proba AS nf_proba_SUB
    , nf_sub.mdl_name AS nf_mdl_name_SUB

    , qb_resp.proba AS qb_proba_RESP
    , qb_resp.mdl_name AS qb_mdl_name_RESP
    , qb_sub.proba AS qb_proba_SUB
    , qb_sub.mdl_name AS qb_mdl_name_SUB

    , CASE WHEN ppp_df.EXTERNAL_ID_MARKETING IS NOT NULL THEN 'Y' ELSE 'N' END AS ppp
    
FROM dat 
    INNER JOIN us_marketing.dnb_core_id_geo_us af
    ON dat.duns = af.duns AND YEAR(dat.load_date) = af.load_year AND MONTH(dat.load_date) = af.load_month
    INNER JOIN us_marketing_features.dnb_us_marketing_basetable b
    ON af.duns = b.duns AND af.load_year = b.load_year AND af.load_month = b.load_month

    LEFT JOIN nf_workarea.historical_campaign_scores nf_resp
    ON dat.duns = nf_resp.duns AND dat.run_date = nf_resp.run_date AND nf_resp.brand = 'NF' AND nf_resp.model_version = '{model_version} RESP'
    LEFT JOIN nf_workarea.historical_campaign_scores nf_sub
    ON dat.duns = nf_sub.duns AND dat.run_date = nf_sub.run_date AND nf_sub.brand = 'NF' AND nf_sub.model_version = '{model_version} SUB'
    LEFT JOIN nf_workarea.historical_campaign_scores qb_resp
    ON dat.duns = qb_resp.duns AND dat.run_date = qb_resp.run_date AND qb_resp.brand = 'QB' AND qb_resp.model_version = '{model_version} RESP'
    LEFT JOIN nf_workarea.historical_campaign_scores qb_sub
    ON dat.duns = qb_sub.duns AND dat.run_date = qb_sub.run_date AND qb_sub.brand = 'QB' AND qb_sub.model_version = '{model_version} SUB'

    LEFT JOIN {db}.matched_companies_02132025_extra_marketing_columns_parsed ppp_df
    ON ppp_df.EXTERNAL_ID_MARKETING=dat.duns
"""

# COMMAND ----------

CTF_PERFORMANCE_QUERY = """
WITH dat as
(
    SELECT *, 'nf' as brand
    FROM {db}.ctf_calib_data_nf_{model_version}
    UNION ALL
    SELECT *, 'qb' as brand
    FROM {db}.ctf_calib_data_qb_{model_version}
)
SELECT dat.*
    , COALESCE(nfp.flg_resp, qbp.flg_resp) AS flg_resp
    , COALESCE(nfp.flg_qual, qbp.flg_qual) AS flg_qual
    , COALESCE(nfp.flg_sub, qbp.flg_sub) AS flg_sub
    , COALESCE(nfp.flg_appr, qbp.flg_appr) AS flg_appr
    , COALESCE(CASE WHEN nfp.internal_approved_amount > 0 THEN 1 ELSE 0 END, CASE WHEN qbp.internal_approved_amount > 0 THEN 1 ELSE 0 END) AS flg_appr_obs
    , COALESCE(nfp.flg_fund, qbp.flg_fund) AS flg_fund
    , COALESCE(CASE WHEN nfp.internal_funded_amount > 0 THEN 1 ELSE 0 END, CASE WHEN qbp.internal_funded_amount > 0 THEN 1 ELSE 0 END) AS flg_fund_obs
    , COALESCE(nfp.fund_amt, qbp.fund_amt) AS fund_amt
    , COALESCE(nfp.internal_funded_amount, qbp.internal_funded_amount) AS fund_amt_obs
    
FROM dat 
    LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_nf_2025111 nfp
    ON dat.accountnum=nfp.accountnum AND dat.brand='nf' AND nfp.RUN_DATE = '{run_date}'
    LEFT JOIN nf_dev_workarea.load_as_df_campaign_performance_qb_2025111 qbp
    ON dat.accountnum=qbp.accountnum AND dat.brand='qb' AND qbp.RUN_DATE = '{run_date}'
WHERE dat.mdl_name_RESP IS NOT NULL
"""

# COMMAND ----------

DUNS_CTF_PROJECTION_QUERY = """
 SELECT distinct score.*
    , ctf_nf.rate_resp as nf_rate_resp
    , ctf_nf.rate_qual as nf_rate_qual
    , ctf_nf.rate_sub  as nf_rate_sub
    , ctf_nf.rate_appr  as nf_rate_appr
    , ctf_nf.rate_fund  as nf_rate_fund
    , ctf_nf.fund_amt_per_deal  as nf_fund_amt_per_deal
    , ctf_nf.rate_appr_obs  as nf_rate_appr_obs
    , ctf_nf.rate_fund_obs  as nf_rate_fund_obs
    , ctf_nf.fund_amt_obs_per_deal  as nf_fund_amt_per_deal_obs
    , ctf_nf.CPP  as nf_CPP
    , ctf_qb.rate_resp as qb_rate_resp
    , ctf_qb.rate_qual as qb_rate_qual
    , ctf_qb.rate_sub  as qb_rate_sub
    , ctf_qb.rate_appr  as qb_rate_appr
    , ctf_qb.rate_fund  as qb_rate_fund
    , ctf_qb.fund_amt_per_deal as qb_fund_amt_per_deal
    , ctf_qb.rate_appr_obs  as qb_rate_appr_obs
    , ctf_qb.rate_fund_obs  as qb_rate_fund_obs
    , ctf_qb.fund_amt_obs_per_deal  as qb_fund_amt_per_deal_obs
    , ctf_qb.CPP  as qb_CPP
    , case when score.nf_mailable = True and ctf_nf.brand is not null then ctf_nf.CPP/(ctf_nf.rate_resp*ctf_nf.rate_sub*ctf_nf.rate_appr*ctf_nf.rate_fund*ctf_nf.fund_amt_per_deal) else 999999 end as ctf_nf
    , case when score.qb_mailable = True and ctf_qb.brand is not null then ctf_qb.CPP/(ctf_qb.rate_resp*ctf_qb.rate_sub*ctf_qb.rate_appr*ctf_qb.rate_fund*ctf_qb.fund_amt_per_deal) else 999999 end as ctf_qb
    , case when score.nf_mailable = True and ctf_nf.brand is not null then ctf_nf.CPP/(ctf_nf.rate_resp*ctf_nf.rate_sub*ctf_nf.rate_appr_obs*ctf_nf.rate_fund_obs*ctf_nf.fund_amt_obs_per_deal) else 999999 end as ctf_nf_obs
    , case when score.qb_mailable = True and ctf_qb.brand is not null then ctf_qb.CPP/(ctf_qb.rate_resp*ctf_qb.rate_sub*ctf_qb.rate_appr_obs*ctf_qb.rate_fund_obs*ctf_qb.fund_amt_obs_per_deal) else 999999 end as ctf_qb_obs
    , data.sic2
    , data.sic4
    , ctf_nf.appr_segment AS nf_appr_seg
    , ctf_qb.appr_segment AS qb_appr_seg
    FROM nf_workarea.amp_score_current_{version} score
    inner join us_marketing.dnb_core_id_geo_us af on af.load_year = {tib_suppression_year} and af.load_month = {tib_suppression_month} and score.duns = af.duns
    LEFT JOIN {db}.ctf_calib_tabs_{version} ctf_nf
        ON upper(ctf_nf.brand) = 'NF'
        AND score.class_type = ctf_nf.class_type
        AND score.nf_nlpl_flg = ctf_nf.nlpl_flg
        AND score.list_name = ctf_nf.list_name
        AND score.ste_seg	= ctf_nf.ste_seg
        AND score.mc_flg	= ctf_nf.mc_flg
        AND score.nf_scorebin_RESP = ctf_nf.scorebin_RESP
        AND score.nf_scorebin_SUB	= ctf_nf.scorebin_SUB
        AND score.scorebin_RESP_seg = ctf_nf.scorebin_RESP_seg
        AND score.ccs_points_seg = ctf_nf.ccs_points_seg
        AND score.ppp = ctf_nf.ppp
        AND score.ba_cnt_cr_12m_seg = ctf_nf.ba_cnt_cr_12m_seg
        AND score.inq_inquiry_duns_24m_seg = ctf_nf.inq_inquiry_duns_24m_seg
        AND score.gc_sales_appr_seg = ctf_nf.gc_sales_appr_seg
        AND score.gc_sales_fund_seg = ctf_nf.gc_sales_fund_seg
        AND score.nf_mailable=True
    LEFT JOIN {db}.ctf_calib_tabs_{version} ctf_qb
        ON upper(ctf_qb.brand) = 'QB'
        AND score.class_type = ctf_qb.class_type
        AND score.qb_nlpl_flg = ctf_qb.nlpl_flg
        AND score.list_name = ctf_qb.list_name
        AND score.ste_seg	= ctf_qb.ste_seg
        AND score.mc_flg	= ctf_qb.mc_flg
        AND score.qb_scorebin_RESP = ctf_qb.scorebin_RESP
        AND score.qb_scorebin_SUB	= ctf_qb.scorebin_SUB
        AND score.scorebin_RESP_seg = ctf_qb.scorebin_RESP_seg
        AND score.ccs_points_seg = ctf_qb.ccs_points_seg
        AND score.ppp = ctf_qb.ppp
        AND score.ba_cnt_cr_12m_seg = ctf_qb.ba_cnt_cr_12m_seg
        AND score.inq_inquiry_duns_24m_seg = ctf_qb.inq_inquiry_duns_24m_seg
        AND score.gc_sales_appr_seg = ctf_qb.gc_sales_appr_seg
        AND score.gc_sales_fund_seg = ctf_qb.gc_sales_fund_seg
        AND score.qb_mailable=True
    LEFT JOIN nf_workarea.model_data_input_{version} data
        ON data.duns=score.duns
"""