/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: dispensing_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               PART-D file into CDM DISPENSING table 
*/

create or replace procedure transform_to_dispensing(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/

// collect columns from target cdm encounter table
var collate_col_stmt = snowflake.createStatement({
    sqlText: `SELECT listagg(column_name,',') as enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = current_schema()
                  AND table_name = 'PRIVATE_DISPENSING';`});
var global_cols = collate_col_stmt.execute(); global_cols.next();
var cols_tgt = global_cols.getColumnValue(1).split(",");

// full-load or cdc-based load
var subset_clause = (SRC_SCHEMA === undefined) ? '': `WHERE src_schema = '` + SRC_SCHEMA + `'`;

// generate dynamic dml query
var t_qry = `INSERT INTO private_dispensing
             SELECT pde_id AS dispensingid
                      ,bene_id AS patid
                      ,bene_id || '|' || prscrbid || '|' || to_char(srvc_dt, 'YYYYMMDD') AS prescribingid
                      ,prscrbid AS prescriberid
                      ,prvdr_id AS providerid 
                      ,srvc_dt AS dispense_date
                      ,prdsrvid AS ndc
                      ,dayssply AS dispense_sup
                      ,qtydspns AS dispense_amt
                      ,REGEXP_REPLACE(TRIM(REGEXP_REPLACE(str, '[^[:digit:]|\\\\-|\\\\-|.]', ' ')),' \+',',') AS dispense_dose_disp --"numeric" part of str
                      ,REGEXP_REPLACE(TRIM(REGEXP_REPLACE(str, '[[:digit:]|\\\\-|\\\\-|.]', ' ')),' \+',',') AS dispense_dose_disp_unit --"unit" part of str
                      ,gcdf AS dispense_route
                      ,'CL' AS dispense_source
                      ,prdsrvid AS raw_ndc
                      ,gnn AS raw_rx_med_name
                      ,str AS raw_dispense_dose_disp
                      ,str AS raw_dispense_dose_disp_unit
                      ,gcdf_desc AS raw_dispense_route
                 FROM private_dispensing_stage 
                 ` + subset_clause +`;`;

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry});
run_transform_dml.execute();
commit_txn.execute();  
$$
;


