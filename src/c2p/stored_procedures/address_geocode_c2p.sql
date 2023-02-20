/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: address_geocode_c2p.sql                                                 
# Description: Snowflake Stored Procedure (SP) for transforming 
#              MBSF and claim files and geoid_mapping table into 
#              PRIVATE_ADDRESS_GEOCODE table 
# Note: there is no staging table needed for the transformation.    
*/

create or replace procedure transform_to_private_address_geocode(
    GEO_MAPPING STRING,SRC_KEY STRING)
returns variant
language javascript
as
$$
/*
Dependency: PRIVATE_ADDRESS_HISTORY up to date
@param{string} GEO_MAPPING: name of geocoding mapping source table
@param{string} SRC_KEY: name of source column (from PRIVATE_ADDRESS_HISTORY) used for mapping
*/
// collect target table columns
var collect_tgt_stmt = snowflake.createStatement({
    sqlText: `SELECT table_name,listagg(column_name,',') 
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = current_schema() AND 
                      table_name = 'PRIVATE_ADDRESS_GEOCODE'
                GROUP BY table_name;`});
var get_cols = collect_tgt_stmt.execute(); get_cols.next();
var cols_tgt = get_cols.getColumnValue(2).split(",");
var cols_tgt_mod = cols_tgt.map(item => {return 's.' + item});

// generate dynamic dml query
var t1_qry = `MERGE INTO private_address_geocode t
              USING (
                -- one unique address per row
                WITH cte AS (
                    SELECT DISTINCT addressid, address_county, address_state, address_zip5, address_zip9
                    FROM private_address_history 
                )
                SELECT b.geoid_to AS geocodeid
                      ,a.addressid
                      ,substr(b.geoid_to,1,2) AS geocode_state
                      ,substr(b.geoid_to,1,5) AS geocode_county
                      ,b.geocode_longitude
                      ,b.geocode_latitude
                      ,substr(rpad(b.geoid_to,15,'0'),1,11) AS geocode_tract
                      ,substr(rpad(b.geoid_to,15,'0'),1,12) AS geocode_group
                      ,substr(rpad(b.geoid_to,15,'0'),1,15) AS geocode_block
                      ,a.address_zip9 AS geocode_zip9 -- padding occured at PRIVATE_ADDRESS_HISTORY table
                      ,substr(a.address_zip9,1,5) AS geocode_zip5
                      ,b.geocode_zcta
                      ,b.geocode_custom
                      ,b.geocode_custom_text
                      ,b.shapefile
                      ,b.geo_accuracy
                      ,b.geo_prov_ref
                      ,b.assignment_date::date AS assignment_date
               FROM cte a
               JOIN `+ GEO_MAPPING +` b 
               ON a.`+ SRC_KEY +` = b.geoid_from
               ) s
               ON t.addressid = s.addressid and t.geocodeid = s.geocodeid
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
