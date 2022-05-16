/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: dispensing_stg.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for staging 
#               PART-D file in preparation for CDM DISPENSING table 
*/

create or replace procedure stage_dispensing(SRC_SCHEMA STRING)
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
                  AND table_schema = 'CMS_PCORNET_CDM_STAGING'
                  AND table_name = 'PRIVATE_DISPENSING_STAGE'
                  AND column_name NOT IN ('SRC_SCHEMA','SRC_TABLE');`});
var global_cols = collate_col_stmt.execute(); global_cols.next();
var cols_tgt = global_cols.getColumnValue(1).split(",");

// get source cms table name (name could change over time)
var get_tbl = snowflake.createStatement({
    sqlText: `SELECT DISTINCT table_schema, table_name
                FROM information_schema.tables 
                WHERE table_catalog = 'GROUSE_DB' 
                  AND table_schema = :1
                  AND table_name like '%PDE%';`,
     binds: [SRC_SCHEMA]});
var tables = get_tbl.execute(); tables.next()

// generate dynamic dml query
var table = tables.getColumnValue(2);
var stg_pt_qry = `  INSERT INTO private_dispensing_stage
                    SELECT pde.pde_id
                          ,pde.bene_id
                          ,pde.prscrbid
                          ,pde.srvc_dt
                          ,pde.prdsrvid
                          ,pde.dayssply
                          ,pde.qtydspns
                          ,pde.str
                          ,pde.gcdf -- dosage form
                          ,pde.gcdf_desc
                          ,'` + SRC_SCHEMA + `'
                          ,'` + table + `'
                    FROM ` + SRC_SCHEMA + `.` + table + ` pde;`;                   
                    
// run dynamic dml query
var add_new = snowflake.createStatement({sqlText: stg_pt_qry});
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
add_new.execute();
commit_txn.execute();
$$
;
