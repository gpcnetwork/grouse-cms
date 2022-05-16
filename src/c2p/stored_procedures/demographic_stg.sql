/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: demographic_stg.sql                                                 
# Description: Snowflake Stored Procedure (SP) for staging MBSF denominator files
#              prepared for CDM DEMOGRAPHIC table transformation 
ref: https://github.com/kumc-bmi/grouse/blob/master/etl_i2b2/sql_scripts/cms_enr_dstats.sql
*/

create or replace procedure stage_demographic(SRC_SCHEMA STRING)
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
var get_table = collect_src_stmt.execute();
get_table.next();

// generate dynamic dml query
var src_tbl = get_table.getColumnValue(2);
var stg_qry = `INSERT INTO private_demographic_stage
               SELECT bene_id,
                      bene_dob,
                      sex,
                      race,
                      rti_race_cd,
                      '` + SRC_SCHEMA + `', 
                      '` + src_tbl +`',
                      to_date(replace(rfrnc_yr,',','') || '1231', 'YYYYMMDD') 
               FROM `+ SRC_SCHEMA +`.`+ src_tbl +`;`;

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_stage = snowflake.createStatement({sqlText: stg_qry});
run_stage.execute();
commit_txn.execute();      
$$
;