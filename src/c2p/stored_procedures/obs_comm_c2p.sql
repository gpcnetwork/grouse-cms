/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               public datasets into OBS_COMM table
*/

create or replace procedure transform_to_obs_comm(SRC_TABLE STRING, GEOID_TYPE STRING)
returns variant
language javascript
as
$$
/*
@param{string} SRC_TABLE: the string suggesting source SDOH data table under the common "PUBLIC_DATA_STAGING" schema
@param{string} GEOID_TYPE: one of ('CBG','CT','CNTY','ZIP5') suggesting geographical level of which the obs_comm observation is measured
*/

// full-load or cdc-based load
var subset_clause = (SRC_TABLE === undefined) ? '': `WHERE a.src_table = '` + SRC_TABLE + `'`;

// matching criteria
let matching_clause = `a.geoid_type = '`+ GEOID_TYPE +`'`;
switch(GEOID_TYPE){
    case 'CT':
        subset_clause += ` AND substr(a.geoid,1,11) = substr(b.addressid,1,11)`;
        break;
    case 'CBG':
        subset_clause += ` AND a.geoid = substr(b.addressid,1,12)`;
        break;
    case 'CNTY':
        subset_clause += ` AND substr(a.geoid,1,5) = substr(b.addressid,1,5)`;
        break;
    case 'ZIP5':
        subset_clause += ` AND a.geoid = substr(b.addressid,14,5)`;
        break;
}
// generate dynamic dml query
var t_qry = `INSERT INTO obs_comm
               SELECT DISTINCT 
                      a.geoid
                     ,a.geoid_type
                     ,a.obscomm_code
                     ,a.obscomm_type
                     ,a.obscomm_type_qual
                     ,a.obscomm_result_text
                     ,a.obscomm_result_num
                     ,a.obscomm_result_modifier
                     ,a.obscomm_result_unit
                     ,a.raw_obscomm_result
                     ,a.src_date_start
                     ,a.src_date_end
               FROM CMS_PCORNET_CDM_STAGING.private_obs_comm_stage a
               JOIN lds_address_history b
               ON `+ matching_clause +` `+ subset_clause +`;`;
/**
// preview of the generated dynamic SQL scripts - comment it out when perform actual execution
var log_stmt = snowflake.createStatement({
                sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                binds: [t_qry]});
log_stmt.execute(); 
**/

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform = snowflake.createStatement({sqlText: t_qry});
run_transform.execute();
commit_txn.execute();
$$
;

