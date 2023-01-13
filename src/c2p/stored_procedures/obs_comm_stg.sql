/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_stg.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for staging data from 
#               PUBLIC_DATA_STAGING schema
*/

create or replace procedure stage_obs_comm(SRC_SCHEMA STRING,SRC_TABLE STRING)
returns variant
language javascript
as
$$
// RUCA
if(SRC_TABLE.includes('RUCA')){
    // generate dynamic dml query
    var stg_qry = `INSERT INTO private_obs_comm_stage
                   WITH ruca_cte AS (
                       SELECT fips_ct
                             ,'PRIMARY' AS ruca_type
                             ,RUCA_PRIMARY AS ruca_val
                       FROM `+ SRC_SCHEMA +`.`+ SRC_TABLE +`
                       UNION
                       SELECT fips_ct
                             ,'SECONDARY'
                             ,RUCA_SECONDARY
                       FROM `+ SRC_SCHEMA +`.`+ SRC_TABLE +`
                   )
                   SELECT fips_ct
                         ,'TR' -- follow GEO_ACCURACY valueset
                         ,'RUCA|'||ruca_type
                         ,'UD'
                         ,NULL
                         ,ruca_val
                         ,NULL
                         ,'EQ'
                         ,'score'
                         ,null
                         ,ruca_val
                         ,'`+ SRC_SCHEMA +`'
                         ,'`+ SRC_TABLE +`'
                         ,to_date('2010-01-01')
                         ,current_date
                   FROM ruca_cte;`;
// ADI                    
}else if(SRC_TABLE.includes('ADI')){
    var stg_qry = `INSERT INTO private_obs_comm_stage
                   WITH adi_cte AS (
                       SELECT fips_cbg
                             ,'NATRANK' AS adi_type
                             ,ADI_NATRANK AS adi_val
                       FROM `+ SRC_SCHEMA +`.`+ SRC_TABLE +`
                       UNION
                       SELECT fips_cbg
                             ,'STATERANK'
                             ,ADI_STATERANK
                       FROM `+ SRC_SCHEMA +`.`+ SRC_TABLE +`
                   )
                   SELECT fips_cbg
                         ,'BG' -- follow GEO_ACCURACY valueset
                         ,'ADI|'||adi_type
                         ,'UD'
                         ,NULL
                         ,adi_val
                         ,NULL
                         ,'EQ'
                         ,'rank'
                         ,null
                         ,adi_val
                         ,'`+ SRC_SCHEMA +`'
                         ,'`+ SRC_TABLE +`'
                         ,to_date('2020-01-01')
                         ,current_date
                   FROM adi_cte;`;
// ACS                 
}else if(SRC_TABLE.includes('ACS')){
var collate_col_stmt = snowflake.createStatement({
    sqlText: `SELECT table_name, listagg(column_name,',') within group (ORDER BY column_name) as cols  
                FROM information_schema.columns 
                WHERE table_catalog = 'GROUSE_DB'
                  AND table_schema = '`+ SRC_SCHEMA +`' 
                  AND table_name = '`+ SRC_TABLE +`'
                  AND (column_name like 'CRANE%' OR
                       column_name like '%INDEX')
                GROUP BY table_name;`});
var global_cols = collate_col_stmt.execute(); global_cols.next();
var table = global_cols.getColumnValue(1);
var cols = global_cols.getColumnValue(2); 
var stg_qry = `INSERT INTO private_obs_comm_stage
               SELECT a.fips_ct
                     ,'TR' -- follow GEO_ACCURACY valueset
                     ,b.snomed
                     ,'SM'
                     ,NULL
                     ,NULL
                     ,a.val
                     ,'EQ'
                     ,b.unit
                     ,b.description
                     ,a.val
                     ,'`+ SRC_SCHEMA +`'
                     ,'`+ SRC_TABLE +`'
                     ,to_date('2019-01-01')
                     ,to_date('2019-12-31')
                FROM `+ SRC_SCHEMA +`.`+ SRC_TABLE +`
                UNPIVOT 
                    (val FOR code in (`+ cols +`)) a
                JOIN `+ SRC_SCHEMA +`.ACS_FIELDS b 
                ON a.code = b.code;`;
}
/**
// preview of the generated dynamic SQL scripts - comment it out when perform actual execution
var log_stmt = snowflake.createStatement({
                sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                binds: [stg_qry]});
log_stmt.execute(); 
**/

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_stage = snowflake.createStatement({sqlText: stg_qry});
run_stage.execute();
commit_txn.execute();

$$
;


