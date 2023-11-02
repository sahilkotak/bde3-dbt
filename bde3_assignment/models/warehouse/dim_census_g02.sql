{{ config(materialized='table') }}

SELECT
    lga_code_2016,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    median_tot_hhd_inc_weekly
FROM {{ ref('stg_census_lga_g02') }}