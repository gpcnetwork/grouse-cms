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
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/

// identify source table name (considering potential variations)
var collect_src_stmt = snowflake.createStatement({
    sqlText: `SELECT table_schema, table_name
                FROM information_schema.tables 
                WHERE table_catalog = 'GROUSE_DB' 
                  AND table_name like 'MBSF_AB%'
                  AND table_schema = :1;`,
     binds: [SRC_SCHEMA]});
var get_table = collect_src_stmt.execute(); get_table.next();
var src_tbl = get_table.getColumnValue(2);
var subset_clause = `WHERE src_schema = '` + SRC_SCHEMA + `' AND src_table = '` + src_tbl + `'`;

// generate dynamic dml query
var t_qry = `MERGE INTO private_lds_address_history t
             USING (
                SELECT bene_id || '|' || rfrnc_yr AS addressid
                      ,bene_id AS patid
                      ,'HO' AS address_use
                      ,'OT' AS address_type
                      ,'Y' AS address_preferred
                      ,'NI' AS address_city
                      ,cnty_cd AS address_county
                      ,state_CD AS address_state
                      ,substr(zip_cd,1,5) AS address_zip5
                      ,zip_cd AS address_zip9
                      ,to_date(replace(rfrnc_yr,',','') || '0101', 'YYYYMMDD') AS address_period_start
                      ,to_date(replace(rfrnc_yr,',','') || '1231', 'YYYYMMDD') AS address_period_end
                FROM CMS_PCORNET_CDM_STAGING.private_lds_address_history_stage `+ subset_clause +`
             ) s 
             -- patient consolidation --
             ON t.patid = s.patid AND s.address_zip9 = t.address_zip9
                WHEN MATCHED 
                    THEN UPDATE SET t.address_period_start = LEAST(t.address_period_start,s.address_period_start), t.address_period_end = GREATEST(t.address_period_end,s.address_period_end)
                WHEN NOT MATCHED 
                    THEN INSERT (addressid,patid,address_use,address_type,address_preferred,address_city,address_county,address_state,address_zip5,address_zip9,address_period_start,address_period_end) 
                        VALUES (s.addressid,s.patid,s.address_use,s.address_type,s.address_preferred,s.address_city,s.address_county,s.address_state,s.address_zip5,s.address_zip9,s.address_period_start,s.address_period_end);`;

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry});
run_transform_dml.execute();
commit_txn.execute(); 
$$
;
