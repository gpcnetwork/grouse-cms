/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_c2p.sql                                                 
# Description: Snowflake Stored Procedure (SP) for transforming 
#              MBSF denominator files into CDM ENROLLMENT table structure 
ref: https://github.com/kumc-bmi/grouse/blob/master/etl_i2b2/sql_scripts/cms_enr_dstats.sql
ref: https://resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator
*/
create or replace procedure transform_to_enrollment(PART STRING, STG_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} PART: part name of source enrollment/denominator table
**/

// generate "select" statement based on conditions
var collate_col_stmt = snowflake.createStatement({
    sqlText: `SELECT table_name, listagg(column_name,',') within group (ORDER BY column_name) as cols  
                FROM information_schema.columns 
                WHERE table_catalog = 'GROUSE_DB'
                  AND table_schema = '`+ STG_SCHEMA +`' 
                  AND table_name like '%ENROLLMENT%STAGE%'
                  AND table_name like '%`+ PART +`%'
                GROUP BY table_name;`});
var global_cols = collate_col_stmt.execute(); global_cols.next();
var table = global_cols.getColumnValue(1);
var cols = global_cols.getColumnValue(2);
let t_qry = '';

// generate dynamic dml query
if (PART.includes('AB')){
    const cols_buyin = cols.split(",").filter(value => {return value.includes('BUYIN')});
    const cols_hmo = cols.split(",").filter(value => {return value.includes('HMOIND')});

    t_qry += `MERGE INTO private_enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, buyin, hmo, right(buyin_mo,2) AS mo
                    FROM `+ STG_SCHEMA +`.`+ table +`
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

} else if (PART.includes('C')) {
    const cols_ptc = cols.split(",").filter(value => {return value.includes('CNTRCT')});
    const cols_pbp = cols.split(",").filter(value => {return value.includes('PBPID')});

    t_qry += `MERGE INTO private_enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, buyin, pbp, right(buyin_mo,2) AS mo
                    FROM `+ STG_SCHEMA +`.`+ table +`
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

} else if (PART.includes('D')) {
    const cols_ptd = cols.split(",").filter(value => {return value.includes('CNTRCT')});
    const cols_pbp = cols.split(",").filter(value => {return value.includes('PBP')});
    const cols_rds = cols.split(",").filter(value => {return value.includes('RDSIND')});

    t_qry += `MERGE INTO private_enrollment t
              USING(
                 WITH per_bene_mo AS (
                    SELECT bene_id, rfrnc_yr, buyin, pbp, rds, right(buyin_mo,2) AS mo
                    FROM `+ STG_SCHEMA +`.`+ table +`
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
} 

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry}); 
run_transform_dml.execute();
commit_txn.execute();
$$
;

