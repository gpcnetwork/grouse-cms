/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_stg.sql                                                 
# Description: Snowflake Stored Procedure (SP) for staging MBSF denominator files
#              prepared for CDM ENROLLMENT table transformation 
ref: https://github.com/kumc-bmi/grouse/blob/master/etl_i2b2/sql_scripts/cms_enr_dstats.sql
*/
create or replace procedure stage_enrollment(SRC_SCHEMA STRING, PART STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
 * @param {string} PART: part name of source table
**/

// generate "select" statement based on conditions
var get_tbl = snowflake.createStatement({
    sqlText: `SELECT table_schema, table_name,
                     listagg(column_name,',') within group (ORDER BY column_name) as cols 
                FROM information_schema.columns 
                WHERE table_catalog = 'GROUSE_DB' 
                  AND table_schema = :1
                  AND table_name like 'MBSF%' 
                  AND (REGEXP_LIKE(column_name,'BUYIN([[:digit:]])+') OR
                       column_name like '%HMOIND%' OR
                       column_name like '%CNTRCT%' OR
                       column_name like '%PBP%ID%' OR
                       column_name like '%RDS%IND%')
                GROUP BY table_schema, table_name;`,
     binds: [SRC_SCHEMA]});
var tables = get_tbl.execute();

// for each table
while (tables.next())
{
    // generate dynamic dml query
    var table = tables.getColumnValue(2);
    var cols_var = tables.getColumnValue(3);
    let stg_pt_qry = '';
    
    if (PART.includes('AB') && table.includes('_AB')){
        const cols_ab_local = cols_var.split(",").filter(value => {
            return value.includes('BUYIN') || value.includes('HMOIND')
        });
        // assume staging table columes (ordered as they appear in source table) are aligned with cols_ab_local
        stg_pt_qry += `INSERT INTO private_enrollment_stage_ab
                       SELECT bene_id, rfrnc_yr,`+ cols_ab_local +`,'`+ SRC_SCHEMA +`','`+ table +`',to_date(replace(rfrnc_yr,',','') || '1231', 'YYYYMMDD')
                       FROM `+ SRC_SCHEMA +`.`+ table +`;`;
       
    } else if (PART.includes('C') && table.includes('_ABC')) {
        const cols_c_local = cols_var.split(",").filter(value => {
            return (value.includes('CNTRCT') || value.includes('PBP')) && value.includes('PTC')
        });
        // assume staging table columes (ordered as they appear in source table) are aligned with cols_c_local
        stg_pt_qry += `INSERT INTO private_enrollment_stage_c
                       SELECT bene_id, rfrnc_yr,`+ cols_c_local +`,'`+ SRC_SCHEMA +`','`+ table +`',to_date(replace(rfrnc_yr,',','') || '1231', 'YYYYMMDD')
                       FROM `+ SRC_SCHEMA +`.`+ table +`;`;
                               
    } else if (PART.includes('D') && table.includes('D_')) {
        var cols_d_local = cols_var.split(",").filter(value => {
                return value.includes('CNTRCT') || value.includes('PBP') || value.includes('RDSIND')
        });
        // 'ABCD' table (after verion K) contains similar columns for both part C and D        
        if (table.includes('_ABCD_')){
            cols_d_local = cols_d_local.filter(value => {
                return value.includes('PTD') || value.includes('RDSIND')})
        }; 
        // assume staging table columes (ordered as they appear in source table) are aligned with cols_d_local 
        stg_pt_qry += `INSERT INTO private_enrollment_stage_d
                       SELECT bene_id, rfrnc_yr,`+ cols_d_local +`,'`+ SRC_SCHEMA +`','`+ table +`',to_date(replace(rfrnc_yr,',','') || '1231', 'YYYYMMDD')
                       FROM `+ SRC_SCHEMA +`.`+ table +`;`;   
    } else {
        continue;
    }
    
    // run dynamic dml query
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    var add_new = snowflake.createStatement({sqlText: stg_pt_qry});
    add_new.execute();
    commit_txn.execute();
}
$$
;
