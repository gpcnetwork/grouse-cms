/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid.sql                                                 

safe harbor rule: 
1. random date shifting but consistent at individual level
2. mask birth_date of age > 89 to 1900-01-01

linkage process: 
1. add DOB to bene_mapping table and create DOB_DEID with birth_date masking
2. add random shift column SHIFT to bene_mapping and BENE_ID_HASH
3. create patid_mapping_<site-abbr> for each site with columns: PATID, PAT_DOB, PAT_DOB_DEID, BENE_ID_HASH, BENE_DOB, BENE_DOB_DEID, SHIFT
   3.1. if there is a match with bene_mapping, populate PATID_HASH as BENE_ID_HASH and inherit shift to patid_mapping, 
   3.2. otherwise, generate new PATID_HASH and random SHIFT to patid_mapping
4. create materialized views for all CDMs (cms and site) with aligned patid and add DOB_DEID, SHIFT at the end
*/

create or replace procedure link_deid(SITE STRING)
returns variant
language javascript
as
$$
/**
 * Stored procedure to align cdm patid with cms bene_id (bene_id persists over time)
 * @param {string} SITE: the string of site acronyms (matching schema name suffix)
**/
var cdm_schema = SITE.includes('CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + SITE; 

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
                        ,'LDS_ADDRESS_HISTORY'
                        ,'MED_ADMIN'
                        ,'OBS_CLIN'
                        ,'OBS_GEN'
                        ,'PCORNET_TRIAL'
                        ,'PRESCRIBING'
                        ,'PRO_CM'
                        ,'PROCEDURES'
                        ,'PROVIDER' -- not patient level
                        ,'VITAL'
                        )
                      GROUP BY table_name;`;
var collect_tbl = snowflake.createStatement({sqlText: col_tbl_qry});
var tables = collect_tbl.execute();

// loop over tables
while (tables.next()){   
    // separate date columns and 
    var tbl = tables.getColumnValue(1);
    var cols = tables.getColumnValue(2).split(",").filter(value =>{return !value.includes('RAW') || value.includes('RAW_BASIS')});
    var cols_no_id = cols.filter(value =>{return !value.includes('PATID')});
    var cols_no_id_dob = cols.filter(value =>{return !value.includes('PATID') && !value.includes('BIRTH_DATE')});
    var cols_dt = cols_no_id.filter(value =>{return value.includes('_DATE') && !value.includes('_DATE_IMPUTE') && !value.includes('_DATE_MGMT')});
    var cols_non_dt = cols_no_id.filter(value =>{return !value.includes('_DATE') || value.includes('_DATE_IMPUTE') || value.includes('_DATE_MGMT')});
    
    // add alias to columns
    var cols_no_id_alias = cols_no_id.map(value =>{return 'a.' + value});
    var cols_no_id_dob_alias = cols_no_id_dob.map(value =>{return 'a.' + value});
    var cols_dt_shift_alias = (cols_dt===undefined || cols_dt.length==0) ? cols_no_id : cols_non_dt.map(value =>{return 'a.' + value}) + ',' + cols_dt.map(function(x){return 'a.'+ x + '::date + xw.shift AS ' + x}); 
      
    // construct queries with date shift
    var xw_ref = SITE.includes('CMS') ? 'bene' : 'patid';
    var id_col = SITE.includes('CMS') ? 'bene_id' : 'patid';
    var dob_hash_col = SITE.includes('CMS') ? 'bene_dob_deid' : 'pat_dob_deid';
    
    //no patient-level data, copy as is
    if(tbl.includes('HARVEST') || tbl.includes('LAB_HISTORY') || tbl.includes('PROVIDER')){
        var lds_qry = `CREATE OR REPLACE SECURE VIEW `+ cdm_schema +`.LDS_`+ tbl +` AS
                       SELECT * FROM `+ cdm_schema +`.`+ tbl +`;`;
        var deid_qry = `CREATE OR REPLACE SECURE VIEW `+ cdm_schema +`.DEID_`+ tbl +` AS
                       SELECT * FROM `+ cdm_schema +`.`+ tbl +`;`
    
    //otherwise, need to align patid for both lds and deid view and also shift date for deid view 
    }else{   
        var commit_txn = snowflake.createStatement({sqlText: `commit;`});
        // create secure LDS view (linked)
        var lds_qry = `CREATE OR REPLACE SECURE VIEW `+ cdm_schema +`.LDS_`+ tbl +` AS
                        WITH id_map_cte AS (
                            SELECT xw.`+ id_col +`_hash AS patid, `+ cols_no_id_alias +`
                            FROM `+ cdm_schema +`.`+ tbl +` a
                            JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw
                            ON a.patid = xw.`+ id_col +`
                        )
                        SELECT `+ cols +` FROM id_map_cte;`;
        var run_lds_qry = snowflake.createStatement({sqlText: lds_qry});
        run_lds_qry.execute();
        commit_txn.execute();
                        
        // create secure DEID view
        if(tbl.includes('DEMOGRAPHIC')){                       
            var deid_qry =`CREATE OR REPLACE SECURE VIEW `+ cdm_schema +`.DEID_`+ tbl +` AS
                            WITH deid_cte AS (
                                SELECT xw.`+ id_col +`_hash AS patid, xw.`+ dob_hash_col +` AS birth_date,
                                       `+ cols_no_id_dob_alias +`
                                FROM `+ cdm_schema +`.`+ tbl +` a
                                JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw
                                ON a.patid = xw.`+ id_col +`
                            )
                            SELECT `+ cols +` FROM deid_cte;`;
        }else if(tbl.includes('LDS_ADDRESS_HISTORY')){
            continue;
        }else{
            var deid_qry =`CREATE OR REPLACE SECURE VIEW `+ cdm_schema +`.DEID_`+ tbl +` AS
                            WITH deid_cte AS (
                                SELECT xw.`+ id_col +`_hash AS patid, `+ cols_dt_shift_alias +`
                                FROM `+ cdm_schema +`.`+ tbl +` a
                                JOIN `+ xw_ref +`_mapping.`+ xw_ref +`_xwalk_`+ SITE +` xw
                                ON a.patid = xw.`+ id_col +`
                            )
                            SELECT `+ cols +` FROM deid_cte;`;
        }
    }
    var run_deid_qry = snowflake.createStatement({sqlText: deid_qry});
    run_deid_qry.execute();
    commit_txn.execute();
}
$$
;
