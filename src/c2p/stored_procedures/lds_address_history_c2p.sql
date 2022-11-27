/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_c2p.sql                                                 
# Description: Snowflake Stored Procedure (SP) for transforming 
#              MBSF files into CDM LDS_ADDRESS_HISTORY table 
*/

create or replace procedure transform_to_lds_address_history(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
var subset_clause = `WHERE src_schema = '` + SRC_SCHEMA + `'`;

// generate dynamic dml query
var t_qry = `MERGE INTO lds_address_history t
             USING (
                WITH cte AS (
                    SELECT zip_cd, bene_id,cnty_cd,state_cd,
                           src_date_start,src_date_end,fips_cbg,
                           row_number() over (partition by bene_id, cnty_cd, state_cd, zip_cd, fips_cbg order by src_date_end desc) AS rn
                    FROM CMS_PCORNET_CDM_STAGING.private_lds_address_history_stage 
                         `+ subset_clause +`
                )
                SELECT fips_cbg||'|'||zip_cd||'|'||cnty_cd||'|'||state_cd AS addressid
                      ,bene_id AS patid
                      ,'HO' AS address_use
                      ,'OT' AS address_type
                      ,'Y' AS address_preferred
                      ,'NI' AS address_city
                      ,cnty_cd AS address_county
                      ,state_CD AS address_state
                      ,substr(zip_cd,1,5) AS address_zip5
                      ,zip_cd AS address_zip9
                      ,fips_cbg AS address_fips_cbg
                      ,substr(fips_cbg,1,11) AS address_fips_ct
                      ,substr(fips_cbg,1,5) AS address_fips_cnty
                      ,src_date_start AS address_period_start
                      ,src_date_end AS address_period_end
                FROM cte
                WHERE rn = 1
             ) s 
             -- patient consolidation --
             ON t.patid = s.patid AND s.addressid = t.addressid
                WHEN MATCHED 
                    THEN UPDATE SET t.address_period_start = LEAST(t.address_period_start,s.address_period_start), t.address_period_end = GREATEST(t.address_period_end,s.address_period_end)
                WHEN NOT MATCHED 
                    THEN INSERT (addressid,patid,address_use,address_type,address_preferred,address_city,address_county,address_state,address_zip5,address_zip9,address_fips_cbg,address_fips_ct,address_fips_cnty,address_period_start,address_period_end) 
                        VALUES (s.addressid,s.patid,s.address_use,s.address_type,s.address_preferred,s.address_city,s.address_county,s.address_state,s.address_zip5,s.address_zip9,s.address_fips_cbg,s.address_fips_ct,s.address_fips_cnty,s.address_period_start,s.address_period_end);`;

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
