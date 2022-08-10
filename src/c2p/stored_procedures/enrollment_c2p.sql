/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_c2p.sql                                                 
# Description: Snowflake Stored Procedure (SP) for transforming 
#              MBSF denominator files into CDM ENROLLMENT table structure 
ref: https://github.com/kumc-bmi/grouse/blob/master/etl_i2b2/sql_scripts/cms_enr_dstats.sql
ref: https://resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator
*/
create or replace procedure transform_to_enrollment(PART STRING)
returns variant
language javascript
as
$$
// generate "select" statement based on conditions
var collate_col_stmt = snowflake.createStatement({
    sqlText: `SELECT table_name, listagg(column_name,',') within group (ORDER BY column_name) as cols  
                FROM information_schema.columns 
                WHERE table_catalog = 'GROUSE_DB'
                  AND table_schema = 'CMS_PCORNET_CDM_STAGING' 
                  AND table_name like '%ENROLLMENT%STAGE%'
                  AND table_name like '%`+ PART +`'
                GROUP BY table_name;`});
var global_cols = collate_col_stmt.execute(); global_cols.next();
var table = global_cols.getColumnValue(1);
var cols = global_cols.getColumnValue(2);
let t_qry = '';

// generate dynamic dml query
if (PART =='AB'){
    const cols_buyin = cols.split(",").filter(value => {return value.includes('BUYIN')});
    const cols_hmo = cols.split(",").filter(value => {return value.includes('HMOIND')});

    t_qry += `MERGE INTO enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, buyin, hmo, right(buyin_mo,2) AS mo
                    FROM CMS_PCORNET_CDM_STAGING.`+ table +`
                    -- multi unpivot
                    UNPIVOT 
                     (buyin for buyin_mo in (`+ cols_buyin +`)) buyin_unpvt
                    UNPIVOT 
                     (hmo for hmo_mo in (` + cols_hmo +`)) hmo_unpvt
                    WHERE RIGHT(buyin_mo,2) = RIGHT(hmo_mo,2) AND
                          buyin != '0' -- Not entitled
                    ), ab_cte_cvg AS (
                    SELECT bene_id, rfrnc_yr, mo, buyin, hmo,
                           to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') AS enroll_mo,
                           CASE WHEN hmo in ('0','4') and buyin in ('1', 'A') THEN 'A'
                                WHEN hmo in ('0','4') and buyin in ('2', 'B') THEN 'B'
                                WHEN hmo in ('0','4') and buyin in ('3', 'C') THEN 'AB'
                                ELSE 'MC'
                           END AS coverage
                    FROM per_bene_mo
                    ), ab_cte_cvg_agg AS (
                    SELECT ab_cte_cvg.*,
                           DATEADD(month, - DENSE_RANK() OVER (PARTITION BY bene_id, coverage ORDER BY enroll_mo) + 1,enroll_mo) series
                    FROM ab_cte_cvg
                    )
                    SELECT bene_id,
                           min(enroll_mo) as enr_start_date,
                           DATEADD(month,1,max(enroll_mo))-1 as enr_end_date,
                           'I' as enr_basis,
                           coverage || '|' || LISTAGG(hmo, '') WITHIN GROUP (ORDER BY enroll_mo) AS raw_basis
                    FROM ab_cte_cvg_agg
                    GROUP BY bene_id, coverage, series
                  ) s
            ON t.patid = s.bene_id AND 
               t.enr_basis = s.enr_basis 
            WHEN MATCHED AND t.enr_start_date >= s.enr_start_date AND t.enr_end_date <= s.enr_end_date  
                THEN UPDATE SET t.enr_start_date = s.enr_start_date, t.enr_end_date = s.enr_end_date, t.raw_basis = s.raw_basis
            WHEN NOT MATCHED
                THEN INSERT (PATID,ENR_START_DATE,ENR_END_DATE,CHART,ENR_BASIS,RAW_BASIS) 
                    VALUES (s.bene_id,s.enr_start_date,s.enr_end_date,'Y',s.enr_basis,s.raw_basis);`; 

} else if (PART == 'C') {
    const cols_ptc = cols.split(",").filter(value => {return value.includes('CNTRCT')});
    const cols_pbp = cols.split(",").filter(value => {return value.includes('PBPID')});

    t_qry += `MERGE INTO enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, buyin, pbp, right(buyin_mo,2) AS mo
                    FROM CMS_PCORNET_CDM_STAGING.`+ table +`
                    -- multi unpivot
                    UNPIVOT 
                     (buyin for buyin_mo in (`+ cols_ptc +`)) buyin_unpvt
                    UNPIVOT 
                     (pbp for pbp_mo in (` + cols_pbp +`)) pbp_unpvt
                    WHERE RIGHT(buyin_mo,2) = RIGHT(pbp_mo,2) AND
                          buyin not in ('N','0') -- Not entitled
                    ), c_cte_cvg as (
                    SELECT bene_id, rfrnc_yr, mo, buyin, pbp,
                           to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') as enroll_mo,
                           substr(buyin, 1, 1) as coverage
                    FROM per_bene_mo
                    WHERE substr(buyin, 1, 1) in ('E', 'H', 'R', 'S', 'X') AND 
                          pbp is NOT NULL
                    ), c_cte_cvg_agg as(
                    SELECT c_cte_cvg.*,
                           DATEADD(month, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY enroll_mo) + 1,enroll_mo) series
                    FROM c_cte_cvg
                    )
                    SELECT bene_id,
                           min(enroll_mo) as enr_start_date,
                           DATEADD(month,1,max(enroll_mo))-1 as enr_end_date,
                           'I' as enr_basis,
                           'MC|' || LISTAGG(coverage, '') WITHIN GROUP (ORDER BY enroll_mo) as raw_basis
                    FROM   c_cte_cvg_agg
                    GROUP BY bene_id, series
                  ) s
            ON t.patid = s.bene_id AND 
               t.enr_basis = s.enr_basis 
            WHEN MATCHED AND t.enr_start_date >= s.enr_start_date AND t.enr_end_date <= s.enr_end_date  
                THEN UPDATE SET t.enr_start_date = s.enr_start_date, t.enr_end_date = s.enr_end_date, t.raw_basis = s.raw_basis
            WHEN NOT MATCHED
                THEN INSERT (PATID,ENR_START_DATE,ENR_END_DATE,CHART,ENR_BASIS,RAW_BASIS) 
                    VALUES (s.bene_id,s.enr_start_date,s.enr_end_date,'Y',s.enr_basis,s.raw_basis);`; 

} else if (PART == 'D') {
    const cols_ptd = cols.split(",").filter(value => {return value.includes('CNTRCT')});
    const cols_pbp = cols.split(",").filter(value => {return value.includes('PBP')});
    const cols_rds = cols.split(",").filter(value => {return value.includes('RDSIND')});

    t_qry += `MERGE INTO enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, buyin, pbp, rds, right(buyin_mo,2) AS mo
                    FROM CMS_PCORNET_CDM_STAGING.`+ table +`
                    -- multi unpivot
                    UNPIVOT 
                     (buyin for buyin_mo in (`+ cols_ptd +`)) buyin_unpvt
                    UNPIVOT 
                     (pbp for pbp_mo in (` + cols_pbp +`)) pbp_unpvt
                    UNPIVOT 
                     (rds for rds_mo in (` + cols_rds +`)) rds_unpvt
                    WHERE RIGHT(buyin_mo,2) = RIGHT(pbp_mo,2) AND
                          RIGHT(buyin_mo,2) = RIGHT(rds_mo,2) AND
                          buyin not in ('0','N') -- Not entitled
                    ), d_cte_cvg as (
                    SELECT bene_id, rfrnc_yr, mo, buyin, pbp, rds,
                           to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') as enroll_mo,
                           substr(buyin, 1, 1) as coverage
                    FROM per_bene_mo
                    WHERE substr(buyin, 1, 1) in ('E', 'H', 'R', 'S', 'X') AND 
                          pbp is NOT NULL and rds = 'N'
                    ), d_cte_cvg_agg as(
                    SELECT d_cte_cvg.*,
                           DATEADD(month, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY enroll_mo) + 1,enroll_mo) series
                    FROM d_cte_cvg
                    )
                    SELECT bene_id,
                           min(enroll_mo) as enr_start_date,
                           DATEADD(month,1,max(enroll_mo))-1 as enr_end_date,
                           'D' as enr_basis,
                           'D|' || LISTAGG(coverage, '') WITHIN GROUP (ORDER BY enroll_mo) as raw_basis
                    FROM   d_cte_cvg_agg
                    GROUP BY bene_id, series
                  ) s
            ON t.patid = s.bene_id AND 
               t.enr_basis = s.enr_basis 
            WHEN MATCHED AND t.enr_start_date >= s.enr_start_date AND t.enr_end_date <= s.enr_end_date  
                THEN UPDATE SET t.enr_start_date = s.enr_start_date, t.enr_end_date = s.enr_end_date, t.raw_basis = s.raw_basis
            WHEN NOT MATCHED 
                THEN INSERT (PATID,ENR_START_DATE,ENR_END_DATE,CHART,ENR_BASIS,RAW_BASIS) 
                    VALUES (s.bene_id,s.enr_start_date,s.enr_end_date,'Y',s.enr_basis,s.raw_basis);`;   
                    
} else if (PART == 'DUAL') {
    const cols_dual = cols.split(",").filter(value => {return value.includes('DUAL')});

    t_qry += `MERGE INTO enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, dual, right(dual_mo,2) AS mo
                    FROM CMS_PCORNET_CDM_STAGING.`+ table +`
                    -- unpivot
                    UNPIVOT 
                     (dual for dual_mo in (`+ cols_dual +`)) dual_unpvt
                    WHERE dual not in ('00','09','NA') AND dual is not NULL  -- Not entitled
                    ), dual_cte_cvg as (
                    SELECT bene_id, rfrnc_yr, mo, dual,
                           to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') as enroll_mo,
                           CASE WHEN dual in ('02','04','08') THEN 'F' -- Full
                                WHEN dual in ('01','03','05','06') THEN 'P' -- Partial
                                WHEN dual in ('10') THEN 'C' -- CHIP
                            END AS coverage
                    FROM per_bene_mo
                    ), dual_cte_cvg_agg as(
                    SELECT dual_cte_cvg.*,
                           DATEADD(month, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY enroll_mo) + 1,enroll_mo) series
                    FROM dual_cte_cvg
                    WHERE coverage is not NULL
                    )
                    SELECT bene_id,
                           min(enroll_mo) as enr_start_date,
                           DATEADD(month,1,max(enroll_mo))-1 as enr_end_date,
                           'I' as enr_basis,
                           'DUAL|' || LISTAGG(coverage, '') WITHIN GROUP (ORDER BY enroll_mo) as raw_basis
                    FROM   dual_cte_cvg_agg
                    GROUP BY bene_id, series
                  ) s
            ON t.patid = s.bene_id AND 
               t.enr_basis = s.enr_basis 
            WHEN MATCHED AND t.enr_start_date >= s.enr_start_date AND t.enr_end_date <= s.enr_end_date  
                THEN UPDATE SET t.enr_start_date = s.enr_start_date, t.enr_end_date = s.enr_end_date, t.raw_basis = s.raw_basis
            WHEN NOT MATCHED 
                THEN INSERT (PATID,ENR_START_DATE,ENR_END_DATE,CHART,ENR_BASIS,RAW_BASIS) 
                    VALUES (s.bene_id,s.enr_start_date,s.enr_end_date,'Y',s.enr_basis,s.raw_basis);`;      
                    
} else if (PART == 'LIS') {
    const cols_lis = cols.split(",").filter(value => {return value.includes('CSTSHR')});

    t_qry += `MERGE INTO enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, cstshr, right(cstshr_mo,2) AS mo
                    FROM CMS_PCORNET_CDM_STAGING.`+ table +`
                    -- multi unpivot
                    UNPIVOT 
                     (cstshr for cstshr_mo in (`+ cols_lis +`)) cstshr_unpvt
                    WHERE cstshr not in ('00','09','NA') AND cstshr is not NULL  -- Not entitled
                    ), cstshr_cte_cvg as (
                    SELECT bene_id, rfrnc_yr, mo, cstshr,
                           to_date(replace(rfrnc_yr,',','') || mo, 'YYYYMM') as enroll_mo,
                           CASE WHEN cstshr in ('01','02','03') THEN 'F' -- Full
                                WHEN cstshr in ('04','05','06','07','08') THEN 'P' -- Partial
                                WHEN cstshr in ('10') THEN 'R' -- employer RDS
                            END AS coverage
                    FROM per_bene_mo
                    ), cstshr_cte_cvg_agg as(
                    SELECT cstshr_cte_cvg.*,
                           DATEADD(month, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY enroll_mo) + 1,enroll_mo) series
                    FROM cstshr_cte_cvg
                    WHERE coverage is not NULL
                    )
                    SELECT bene_id,
                           min(enroll_mo) as enr_start_date,
                           DATEADD(month,1,max(enroll_mo))-1 as enr_end_date,
                           'I' as enr_basis,
                           'LIS|' || LISTAGG(coverage, '') WITHIN GROUP (ORDER BY enroll_mo) as raw_basis
                    FROM   cstshr_cte_cvg_agg
                    GROUP BY bene_id, series
                  ) s
            ON t.patid = s.bene_id AND 
               t.enr_basis = s.enr_basis 
            WHEN MATCHED AND t.enr_start_date >= s.enr_start_date AND t.enr_end_date <= s.enr_end_date  
                THEN UPDATE SET t.enr_start_date = s.enr_start_date, t.enr_end_date = s.enr_end_date, t.raw_basis = s.raw_basis
            WHEN NOT MATCHED 
                THEN INSERT (PATID,ENR_START_DATE,ENR_END_DATE,CHART,ENR_BASIS,RAW_BASIS) 
                    VALUES (s.bene_id,s.enr_start_date,s.enr_end_date,'Y',s.enr_basis,s.raw_basis);`;       
} 
/**
// preview of the generated dynamic SQL scripts - comment it out when perform actual execution
   var log_stmt = snowflake.createStatement({
                    sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                    binds: [t_qry]});
   log_stmt.execute(); 
**/
// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry}); 
run_transform_dml.execute();
commit_txn.execute();
$$
;
