/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid.sql                                                 
# Description: stored procedures to link and deid individuals, 
*/

create or replace procedure link_deid(
    SITE STRING, SINGLE_TABLE STRING)
returns variant
language javascript
as
$$
/**
 * Stored procedure to align cdm patid with cms bene_id (bene_id persists over time)
 * @param {string} SITE: the string of site acronyms (matching schema name suffix)
 * @param {string} SINGLE_TABLE: only deidentify a single table
**/
var cdm_schema = SITE.includes('CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + SITE; 
var single_tbl = (SINGLE_TABLE === undefined) ? '': ` AND table_name IN ('`+ SINGLE_TABLE + `')`; 

// collect table and column names
var col_tbl_qry = `SELECT table_name, listagg(column_name,',') AS col_lst
                    FROM information_schema.columns 
                    WHERE table_catalog = 'GROUSE_DB' 
                      AND table_schema = '`+ cdm_schema +`'
                      AND table_name IN (
                         'CONDITION'
                        ,'DEATH_CAUSE'
                        ,'DEATH'
                        ,'DEMOGRAPHIC'
                        ,'DIAGNOSIS'
                        ,'DISPENSING'
                        ,'ENCOUNTER'
                        ,'ENROLLMENT'
                        ,'HARVEST'  -- not patient level
                        ,'HASH_TOKEN'
                        ,'IMMUNIZATION'
                        ,'LAB_HISTORY' -- not patient level
                        ,'LAB_RESULT_CM'
                        ,'PRIVATE_ADDRESS_HISTORY'
                        ,'PRIVATE_ADDRESS_GEOCODE'
                        ,'MED_ADMIN'
                        ,'OBS_CLIN'
                        ,'OBS_GEN'
                        ,'PCORNET_TRIAL'
                        ,'PRESCRIBING'
                        ,'PRO_CM'
                        ,'PROCEDURES'
                        ,'PROVIDER' -- not patient level
                        ,'VITAL'
                        ,'OBS_COMM'
                        ) `+ single_tbl +`
                      GROUP BY table_name;`;
var collect_tbl = snowflake.createStatement({sqlText: col_tbl_qry});
var tables = collect_tbl.execute();

// loop over tables
while (tables.next()){   
    // get table name, complete column names for different de-id scenario
    var tbl = tables.getColumnValue(1);
    var cols = tables.getColumnValue(2).split(",").filter(value =>{return !value.includes('RAW_') || value.includes('RAW_RX_MED_NAME') || value.includes('RAW_BASIS') || value.includes('RAW_OBSCOMM_NAME')});
    var cols_no_addr = cols.filter(value =>{return !value.includes('DETAIL') && !value.includes('STREET') && !value.includes('LONGITUDE')  && !value.includes('LATITUDE') && !value.includes('GEOCODE_CUSTOM') && !value.includes('GEOCODE_BLOCK')});
    var cols_no_addr_deid = cols_no_addr.filter(value =>{return !value.includes('ZIP') && !value.includes('CITY') && !value.includes('COUNTY')});
    
    // separate out id and dob columns 
    var cols_no_id = cols.filter(value =>{return !value.includes('PATID') && !value.includes('ADDRESSID') && !value.includes('GEOCODEID')});
    var cols_no_id_dob = cols.filter(value =>{return !value.includes('PATID') && !value.includes('ADDRESSID') && !value.includes('GEOCODEID') && !value.includes('BIRTH_DATE')});
    
    // separate out date columns and protected columns
    var cols_dt = cols_no_id.filter(value =>{return (value.includes('_DATE') || value.includes('_PERIOD_')) && !value.includes('_DATE_IMPUTE') && !value.includes('_DATE_MGMT')});
    var cols_non_dt = cols_no_id.filter(value =>{return (!value.includes('_DATE') && !value.includes('_PERIOD_')) || value.includes('_DATE_IMPUTE') || value.includes('_DATE_MGMT')});
    
    // add alias and shifted function to protected columns
    var cols_no_id_alias = cols_no_id.map(value =>{return 'a.' + value});
    var cols_no_id_dob_alias = cols_no_id_dob.map(value =>{return 'a.' + value});
    var cols_dt_shift_alias = (cols_dt===undefined || cols_dt.length==0) ? cols_no_id : cols_non_dt.map(value =>{return 'a.' + value}) + ',' + cols_dt.map(function(x){return 'a.'+ x + '::date + xw.shift AS ' + x});
      
    // construct queries with date shift
    var xw_ref = SITE.includes('CMS') ? 'bene' : 'patid';
    var id_col = SITE.includes('CMS') ? 'bene_id' : 'patid';
    var dob_hash_col = SITE.includes('CMS') ? 'bene_dob_deid' : 'pat_dob_deid';
    
    //no patient-level or geo-level data, copy as is
    if(tbl.includes('HARVEST') || tbl.includes('LAB_HISTORY') || tbl.includes('PROVIDER')){
        var lds_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.LDS_`+ tbl +` AS
                       SELECT * FROM `+ cdm_schema +`.`+ tbl +`;`;
        var deid_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.DEID_`+ tbl +` AS
                       SELECT * FROM `+ cdm_schema +`.`+ tbl +`;`
    
    //otherwise, need to align patid for both lds and deid table and also shift date for deid table 
    }else{   
        var commit_txn = snowflake.createStatement({sqlText: `commit;`});
        // create secure LDS table (linked)
        var lds_tbl = tbl.includes('PRIVATE') ? tbl.replace('PRIVATE','LDS'): `LDS_`+ tbl.replace('LDS_','');
        
        // need to deidentify patid, addressid, geocodeoid
        if(tbl.includes('ADDRESS_HISTORY')){   
            var lds_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ lds_tbl +` AS
                                WITH id_map_cte AS (
                                    SELECT DISTINCT 
                                           xw.`+ id_col +`_hash AS patid, axw.addressid_hash AS addressid, 
                                           `+ cols_no_id_alias +`
                                    FROM `+ cdm_schema +`.`+ tbl +` a
                                    JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw ON a.patid = xw.`+ id_col +`
                                    JOIN geoid_mapping.addressid_xwalk_`+ SITE +` axw ON a.addressid = axw.addressid
                                )
                                SELECT `+ cols_no_addr +` FROM id_map_cte;`;
                                
        // need to deidentify addressid, geocodeid
        }else if(tbl.includes('ADDRESS_GEOCODE')){
            var lds_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ lds_tbl +` AS
                                SELECT DISTINCT 
                                       gxw.geoid_hash as geocodeid,
                                       axw.addressid_hash as addressid,
                                       a.geocode_state,
                                       a.geocode_county,
                                       a.geocode_tract,
                                       gxw2.geoid_hash AS geocode_group,
                                       gxw3.geoid_hash AS geocode_zip9,
                                       a.geocode_zip5,
                                       a.geocode_zcta, 
                                       a.shapefile AS shapefile,
                                       'Z9' as geo_accuracy,
                                       a.geo_prov_ref, 
                                       a.assignment_date 
                                FROM `+ cdm_schema +`.`+ tbl +` a
                                JOIN geoid_mapping.addressid_xwalk_`+ SITE +` axw ON a.addressid = axw.addressid
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw ON a.geocodeid = gxw.geoid AND gxw.geoid_type = 'GEOCODEID'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw2 ON a.geocode_group = gxw2.geoid AND gxw2.geoid_type = 'GEOCODE_GROUP'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw3 ON a.geocode_zip9 = gxw3.geoid AND gxw3.geoid_type = 'GEOCODE_ZIP9'
                                ;`;
                                    
        // need to deidentify geocodeid
        }else if(tbl.includes('OBS_COMM')){
            var lds_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ lds_tbl +` AS
                                    WITH id_map_cte AS (
                                        SELECT DISTINCT 
                                               xw.geoid_hash AS obscomm_geocodeid,`+ cols_no_id_alias +`
                                        FROM `+ cdm_schema +`.`+ tbl +` a
                                        JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` xw 
                                        ON a.obscomm_geocodeid = xw.geoid AND a.obscomm_geo_accuracy = xw.geo_accuracy
                                    )
                                    SELECT `+ cols_no_addr +` FROM id_map_cte;`;
        
        // need to deidentify patid
        }else{
            var lds_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ lds_tbl +` AS
                                WITH id_map_cte AS (
                                    SELECT DISTINCT 
                                           xw.`+ id_col +`_hash AS patid, `+ cols_no_id_alias +`
                                    FROM `+ cdm_schema +`.`+ tbl +` a
                                    JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw
                                    ON a.patid = xw.`+ id_col +`
                                )
                                SELECT `+ cols +` FROM id_map_cte;`;
        }
        
        var run_lds_qry = snowflake.createStatement({sqlText: lds_t_qry});
        run_lds_qry.execute();
        commit_txn.execute();
                        
        // create secure DEID table
        var deid_tbl = tbl.includes('PRIVATE') ? tbl.replace('PRIVATE','DEID'): `DEID_`+ tbl;
        
        // need to deidentify patid and birth_date
        if(tbl.includes('DEMOGRAPHIC')){     
            var deid_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ deid_tbl +` AS
                                WITH deid_cte AS (
                                    SELECT DISTINCT 
                                           xw.`+ id_col +`_hash AS patid, xw.`+ dob_hash_col +` AS birth_date,
                                           `+ cols_no_id_dob_alias +`
                                    FROM `+ cdm_schema +`.`+ tbl +` a
                                    JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw 
                                    ON a.patid = xw.`+ id_col +`
                                )
                                SELECT `+ cols +` FROM deid_cte;`;
        
        // need to deidentify patid, addressid, geocodeid, and all dates
        }else if(tbl.includes('ADDRESS_HISTORY')){
            var deid_t_qry =`CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ deid_tbl +` AS
                                WITH deid_cte AS (
                                    SELECT DISTINCT 
                                           xw.`+ id_col +`_hash AS patid, axw.addressid_hash AS addressid, 
                                           `+ cols_dt_shift_alias +`
                                    FROM `+ cdm_schema +`.`+ tbl +` a
                                    JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw ON a.patid = xw.`+ id_col +`
                                    JOIN geoid_mapping.addressid_xwalk_`+ SITE +` axw ON a.addressid = axw.addressid
                                )
                                SELECT `+ cols_no_addr_deid +` FROM deid_cte;`;
                                
        // need to hash more geoid columns, but no dates need to be shifted
        }else if(tbl.includes('ADDRESS_GEOCODE')){
            var deid_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ deid_tbl +` AS
                                    SELECT DISTINCT 
                                       gxw.geoid_hash as geocodeid,
                                       axw.addressid_hash as addressid,
                                       a.geocode_state,
                                       gxw6.geoid_hash AS geocode_county,
                                       gxw4.geoid_hash AS geocode_tract,
                                       gxw2.geoid_hash AS geocode_group,
                                       gxw3.geoid_hash AS geocode_zip9,
                                       gxw5.geoid_hash AS geocode_zip5,
                                       a.geocode_zcta, 
                                       a.shapefile AS shapefile,
                                       'Z9' as geo_accuracy,
                                       a.geo_prov_ref, 
                                       a.assignment_date 
                                FROM `+ cdm_schema +`.`+ tbl +` a
                                JOIN geoid_mapping.addressid_xwalk_`+ SITE +` axw ON a.addressid = axw.addressid
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw ON a.geocodeid = gxw.geoid AND gxw.geoid_type = 'GEOCODEID'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw2 ON a.geocode_group = gxw2.geoid AND gxw2.geoid_type = 'GEOCODE_GROUP'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw3 ON a.geocode_zip9 = gxw3.geoid AND gxw3.geoid_type = 'GEOCODE_ZIP9'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw4 ON a.geocode_tract = gxw4.geoid AND gxw4.geoid_type = 'GEOCODE_TRACT'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw5 ON a.geocode_zip5 = gxw5.geoid AND gxw5.geoid_type = 'GEOCODE_ZIP5'
                                JOIN geoid_mapping.geocodeid_xwalk_`+ SITE +` gxw6 ON a.geocode_county = gxw6.geoid AND gxw6.geoid_type = 'GEOCODE_COUNTY'
                                ;`;
       // similar to LDS with more location-sensitive columns removed 
       }else if (tbl.includes('OBS_COMM')){
            var deid_t_qry = `CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ deid_tbl +` AS
                                    SELECT `+ cols_no_addr_deid +` 
                                    FROM `+ cdm_schema +`.`+ lds_tbl +`;`;
   
       // need to deidentify patid and all real dates                      
        }else{
            var deid_t_qry =`CREATE OR REPLACE TABLE `+ cdm_schema +`.`+ deid_tbl +` AS
                                WITH deid_cte AS (
                                    SELECT DISTINCT 
                                           xw.`+ id_col +`_hash AS patid, `+ cols_dt_shift_alias +`
                                    FROM `+ cdm_schema +`.`+ tbl +` a
                                    JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw
                                    ON a.patid = xw.`+ id_col +`
                                )
                                SELECT `+ cols +` FROM deid_cte;`;
        }
    }
    var run_deid_t_qry = snowflake.createStatement({sqlText: deid_t_qry});
    run_deid_t_qry.execute();
    commit_txn.execute();
}
$$
;

create or replace procedure gen_deid_view(SITE STRING)
returns variant
language javascript
as
$$
/**
 * Stored procedure to generate de-id views
 * @param {string} SITE: the string of site acronyms (matching schema name suffix)
**/ 
// check if target deid table exists
var cdm_schema = SITE.includes('CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + SITE;
var chk_tbl = `SELECT DISTINCT table_name
               FROM information_schema.tables 
               WHERE table_schema = '`+ cdm_schema +`' 
                 AND table_name like 'DEID%'`;
var run_chk_tbl = snowflake.createStatement({sqlText: chk_tbl});
var get_tbl_result = run_chk_tbl.execute(); 

// loop over tables
while(get_tbl_result.next()){ 
    // create view name
    var deid_tbl = get_tbl_result.getColumnValue(1); 
    var deid_view = `V_` + deid_tbl;
    
    // view genertion query
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    var deid_v_qry =`CREATE OR REPLACE SECURE VIEW `+ cdm_schema +`.`+ deid_view +` AS
                     SELECT * FROM `+ deid_tbl +`;`;
    var run_deid_v_qry = snowflake.createStatement({sqlText: deid_v_qry});
    run_deid_v_qry.execute();
    commit_txn.execute();
}
$$
;
