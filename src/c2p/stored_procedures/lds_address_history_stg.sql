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
                       WITH zip_clean_cte AS (
                        SELECT bene_id,cnty_cd,state_cd,rfrnc_yr,
                               rpad(`+ col_zip +`,9,'0') AS zip9
                        FROM `+ SRC_SCHEMA +`.`+ src_tbl +`
                        WHERE trim(`+ col_zip +`) <> '' AND
                              trim(`+ col_zip +`) <> '0000' AND
                              `+ col_zip +` is not null
                       )
                       SELECT a.bene_id,
                              a.cnty_cd,
                              a.state_cd,
                              a.zip9,
                              b.fips,
                              '` + SRC_SCHEMA + `',
                              '` + src_tbl + `',
                              to_date(replace(a.rfrnc_yr,',','') || '0101', 'YYYYMMDD'),
                              to_date(replace(a.rfrnc_yr,',','') || '1231', 'YYYYMMDD'),
                              1,
                              case when substr(a.zip9,6,4) = '0000' then 0.5
                                   else 1 end
                       FROM zip_clean_cte a
                       LEFT JOIN geoid_mapping.zip9_mapto_cbg b -- schema and table names are hard-coded
                       ON a.zip9 = b.zip_4
                       WHERE a.zip9 is not null;`;
        }else{
        // pick up additional address information from other claims files
        var stg_qry = `INSERT INTO private_lds_address_history_stage
                       WITH zip_clean_cte AS (
                        SELECT bene_id,cnty_cd,state_cd,
                               rpad(`+ col_zip +`,9,'0') AS zip9,
                               min(from_dt) AS start_dt,
                               max(from_dt) AS end_dt
                        FROM `+ SRC_SCHEMA +`.`+ src_tbl +`
                        WHERE trim(`+ col_zip +`) <> '' AND
                              trim(`+ col_zip +`) <> '0000' AND
                              `+ col_zip +` is not null
                        GROUP BY bene_id,cnty_cd,state_cd,`+ col_zip +`
                       )
                       SELECT a.bene_id,
                              a.cnty_cd,
                              a.state_cd,
                              a.zip9,
                              b.fips,
                              '` + SRC_SCHEMA + `',
                              '` + src_tbl + `',
                              a.start_dt,
                              a.end_dt,
                              0.5,
                              case when substr(a.zip9,6,4) = '0000' then 0.5
                                   else 1 end
                       FROM zip_clean_cte a
                       LEFT JOIN geoid_mapping.zip9_mapto_cbg b -- schema and table names are hard-coded
                       ON a.zip9 = b.zip_4
                       WHERE a.zip9 is not null;`;
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
    
}
$$
;

