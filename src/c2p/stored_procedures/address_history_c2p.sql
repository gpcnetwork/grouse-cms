/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: address_history_c2p.sql                                                 
# Description: Snowflake Stored Procedure (SP) for transforming 
#              MBSF and claim files into CDM LDS_ADDRESS_HISTORY table 
*/

create or replace procedure transform_to_private_address_history(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/*
@param{string} SRC_SCHEMA: the string suggesting source schema
*/

var subset_clause = `WHERE src_schema = '` + SRC_SCHEMA + `'`;

// collect target table columns
var collect_tgt_stmt = snowflake.createStatement({
    sqlText: `SELECT table_name,listagg(column_name,',') as enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = current_schema()  
                  AND table_name = 'PRIVATE_ADDRESS_HISTORY'
                GROUP BY table_name;`});
var get_cols = collect_tgt_stmt.execute(); get_cols.next();
var cols_tgt = get_cols.getColumnValue(2).split(",");
var cols_tgt_mod = cols_tgt.map(item => {return 's.' + item});

// generate dynamic dml query
var t1_qry = `MERGE INTO private_address_history t
              USING (
                WITH cte AS (
                    SELECT zip_cd, bene_id,cnty_cd,state_cd,
                           src_date_start,src_date_end,
                           row_number() over (partition by bene_id, cnty_cd, state_cd, zip_cd order by src_date_end desc) AS rn
                    FROM CMS_PCORNET_CDM_STAGING.private_address_history_stage 
                         `+ subset_clause +`
                )
                SELECT zip_cd||'|'||cnty_cd||'|'||state_cd AS addressid -- consistent construct
                      ,bene_id AS patid
                      ,'HO' AS address_use
                      ,'OT' AS address_type
                      ,'Y' AS address_preferred
                      ,NULL AS address_street
                      ,NULL AS address_detail
                      ,'NI' AS address_city
                      ,cnty_cd AS address_county
                      ,state_CD AS address_state
                      ,substr(zip_cd,1,5) AS address_zip5
                      ,zip_cd AS address_zip9
                      ,src_date_start AS address_period_start
                      ,src_date_end AS address_period_end
                      ,NULL AS raw_address_text
                FROM cte
                WHERE rn = 1
             ) s 
             -- patient,address consolidation --
             ON t.patid = s.patid AND s.addressid = t.addressid
                WHEN MATCHED 
                    THEN UPDATE SET t.address_period_start = LEAST(t.address_period_start,s.address_period_start), t.address_period_end = GREATEST(t.address_period_end,s.address_period_end)
                WHEN NOT MATCHED 
                    THEN INSERT (`+ cols_tgt +`) VALUES (`+ cols_tgt_mod +`);`;

/**
// preview of the generated dynamic SQL scripts - comment it out when perform actual execution
var log_stmt = snowflake.createStatement({
                sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                binds: [t_qry]});
log_stmt.execute(); 
**/

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t1_qry});
run_transform_dml.execute();
commit_txn.execute(); 
$$
;
