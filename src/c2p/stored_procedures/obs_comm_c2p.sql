/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               public datasets into OBS_COMM table
*/

create or replace procedure transform_to_obs_comm(
    SRC_TABLE STRING, GEO_ACCURACY STRING)
returns variant
language javascript
as
$$
/*
@param{string} SRC_TABLE: the string suggesting source SDOH data table under the common "PUBLIC_DATA_STAGING" schema
@param{string} GEO_ACCURACY: one of ('BG','TR','CN','Z5','ST') suggesting geographical level of which the obs_comm observation is measured
*/
// one source at a time
var subset_clause = `WHERE a.src_table = '` + SRC_TABLE + `'`;

// matching criteria
let matching_clause = `a.obscomm_geo_accuracy = '`+ GEO_ACCURACY +`'`;
switch(GEO_ACCURACY){
    case 'BG':
        subset_clause += ` AND a.geocodeid = b.geocode_group`;
        break;
    case 'TR':
        subset_clause += ` AND a.geocodeid = b.geocode_tract`;
        break;
    case 'CN':
        subset_clause += ` AND a.geocodeid = b.geocode_county`;
        break;
    case 'ST':
        subset_clause += ` AND a.geocodeid = b.geocode_state`;
        break;
    case 'Z5':
        subset_clause += ` AND a.geocodeid = substr(b.geocode_zip,1,5)`;
        break;
}
// generate dynamic dml query
var t_qry = `INSERT INTO obs_comm
               SELECT DISTINCT 
                      a.geocodeid
                     ,a.obscomm_geo_accuracy
                     ,a.obscomm_code
                     ,a.obscomm_type
                     ,a.obscomm_type_qual
                     ,a.obscomm_result_text
                     ,a.obscomm_result_num
                     ,a.obscomm_result_modifier
                     ,a.obscomm_result_unit
                     ,a.raw_obscomm_name
                     ,a.raw_obscomm_result
                    --  ,a.src_date_start
                    --  ,a.src_date_end
               FROM CMS_PCORNET_CDM_STAGING.private_obs_comm_stage a
               JOIN private_address_geocode b
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
