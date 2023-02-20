/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_stg.sql                                                 
# Description: Snowflake Stored Procedure (SP) for staging MBSF denominator files
#              prepared for CDM LDS_ADDRESS_HISTORY table transformation 
*/

create or replace procedure stage_lds_address_history(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
// identify source table name (considering potential variations)
var collect_src_stmt = snowflake.createStatement({
    sqlText: `SELECT table_schema, table_name, column_name 
                FROM information_schema.columns 
                WHERE table_catalog = 'GROUSE_DB' 
                  AND (table_name like 'MBSF_AB%' OR
                       table_name in ('HHA_BASE_CLAIMS',
                                      'HOSPICE_BASE_CLAIMS',
                                      'OUTPATIENT_BASE_CLAIMS',
                                      'BCARRIER_CLAIMS',
                                      'DME_CLAIMS'))
                  AND table_schema = :1
                  AND column_name like '%ZIP%';`,
     binds: [SRC_SCHEMA]});
var get_table = collect_src_stmt.execute();

while(get_table.next()){
    var src_tbl = get_table.getColumnValue(2);
    var col_zip = get_table.getColumnValue(3);
    
    if(src_tbl.includes('MBSF')){
        // generate dynamic dml query
        var stg_qry = `INSERT INTO private_lds_address_history_stage
                       SELECT bene_id,
                              rfrnc_yr,
                              CNTY_CD,
                              STATE_CD,
                              `+ col_zip +`,
                              '` + SRC_SCHEMA + `',
                              '` + src_tbl + `',
                              to_date(replace(rfrnc_yr,',','') || '1231', 'YYYYMMDD'),
                              1
                       FROM `+ SRC_SCHEMA +`.`+ src_tbl +`;`;
        }else{
        // pick up additional address information from other claims files
        var stg_qry = `INSERT INTO private_lds_address_history_stage
                       SELECT bene_id,
                              year(from_dt),
                              CNTY_CD,
                              STATE_CD,
                              `+ col_zip +`,
                              '` + SRC_SCHEMA + `',
                              '` + src_tbl + `',
                              from_dt,
                              0.5
                       FROM `+ SRC_SCHEMA +`.`+ src_tbl +`;`;
   }

    // run dynamic dml query
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    var run_stage = snowflake.createStatement({sqlText: stg_qry});
    run_stage.execute();
    commit_txn.execute();
}
$$
;

