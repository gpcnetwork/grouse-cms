/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid_stg.sql                                                 
# Description: Staging crosswalk files to link CDM data with CMS data
*/

create or replace procedure link_deid_stg(
    SITE STRING,
    SEED FLOAT,
    NEW_XWALK STRING,
    NEW_XWALK_HASH_COLNM STRING,
    SITE_XWALK STRING
)
returns variant
language javascript
as
$$
/**
 * Stored procedure to align cdm patid with cms bene_id (bene_id persists over time)
 * @param {string} SITE: the string of site acronyms (matching schema name suffix)
 * @param {integer} SEED: seed number for generating random day shifts
 * @param(optional) {string} NEW_XWALK: name of new crosswalk file, useful for CMS CDM but not needed for site CDM
 * @param(optional) {string} NEW_XWALK_HASH_COLNM: column name of HASHID column in the new crosswalk file, useful for CMS CDM but not needed for site CDM
 * @param(optional) {string} SITE_XWALK: name of the optional site crosswalk to CDM PATID
**/
let new_tmp_qry = '';
let stg_qry = '';
let join_xwalk_clause = '';

// for cms-cdm
if(SITE.includes('CMS')){
    //
    new_tmp_qry += `CREATE OR REPLACE TABLE bene_mapping.bene_xwalk_tmp AS
                        WITH dob_shift_cte AS (
                            SELECT a.patid AS bene_id
                                  ,md5(a.patid) AS bene_id_hash
                                  ,a.birth_date AS bene_dob
                                  ,b.`+ NEW_XWALK_HASH_COLNM +` AS hashid
                                  ,b.site_id AS siteid
                                  ,-uniform(1, 365, random(`+ SEED +`)) AS shift
                                  ,`+ SEED +` AS seed
                          FROM cms_pcornet_cdm.demographic a
                          LEFT JOIN bene_mapping.`+ NEW_XWALK +` b 
                          ON a.patid = b.bene_id
                          WHERE b.unique_match = 1 -- double check field name before runing
                        )
                        SELECT bene_id
                              ,bene_id_hash
                              ,bene_dob
                              ,CASE WHEN round(datediff(day,bene_dob,current_date)/365.25)+1 > 89 THEN TO_DATE('1900-01-01')
                                    ELSE dateadd(day,shift,bene_dob::date)
                               END AS bene_dob_deid
                              ,hashid
                              ,CASE WHEN siteid = 'UMO' THEN 'MU'
                                    WHEN siteid = 'UN' THEN 'UNMC'
                                    WHEN siteid = 'MCRF' THEN 'MCRI'
                                    WHEN siteid = 'UK' THEN 'KUMC'
                                    WHEN siteid = 'AH' THEN 'ALLINA'
                                    WHEN siteid = 'WU' THEN 'WASHU'
                                    ELSE siteid END AS siteid
                              ,shift
                              ,seed
                      FROM dob_shift_cte;`;
    //                  
    stg_qry += `MERGE INTO bene_mapping.bene_xwalk_cms t
                    USING (SELECT * FROM bene_mapping.bene_xwalk_tmp) s
                    ON t.bene_id = s.bene_id
                    WHEN MATCHED AND t.bene_dob_deid > TO_DATE('1900-01-01') AND s.bene_dob_deid = TO_DATE('1900-01-01')
                        THEN UPDATE SET t.bene_dob_deid = s.bene_dob_deid
                    WHEN NOT MATCHED
                        THEN INSERT (bene_id,bene_id_hash,bene_dob,bene_dob_deid,hashid,siteid,shift,seed) 
                            VALUES (s.bene_id,s.bene_id_hash,s.bene_dob,s.bene_dob_deid,s.hashid,s.siteid,s.shift,s.seed);`;
// for site-cdm    
} else {
    // optional join with intermediate CDM PATID mapping
    if(SITE_XWALK===undefined || SITE_XWALK===null){
        join_xwalk_clause += `LEFT JOIN bene_mapping.bene_xwalk_cms b ON a.patid = b.hashid AND b.siteid = '`+ SITE +`'`
    }else{
        join_xwalk_clause += `LEFT JOIN pcornet_cdm_`+ SITE +`.`+ SITE_XWALK +` xw ON a.patid = xw.patid 
                              LEFT JOIN bene_mapping.bene_xwalk_cms b ON xw.hashid = b.hashid AND b.siteid = '`+ SITE +`'`
    }
    
    //create patid_mapping_SITE table
    new_tmp_qry += `CREATE OR REPLACE TABLE patid_mapping.patid_xwalk_tmp AS
                        WITH dob_shift_cte AS (
                            SELECT a.patid
                                  ,CASE WHEN b.bene_id_hash is not null THEN b.bene_id_hash
                                        ELSE md5('`+ SITE +`_' || a.patid) || 'z'
                                   END AS patid_hash 
                                  ,a.birth_date AS pat_dob
                                  ,CASE WHEN b.shift is not null THEN b.shift
                                        ELSE -uniform(1, 365, random(`+ SEED +`))
                                   END AS shift
                                  ,`+ SEED +` AS seed
                            FROM pcornet_cdm_`+ SITE +`.demographic a
                            `+ join_xwalk_clause +`
                        )
                        SELECT patid
                              ,patid_hash 
                              ,pat_dob
                              ,CASE WHEN round(datediff(day,pat_dob,current_date)/365.25)+1 > 89 THEN '1900-01-01'
                                    ELSE dateadd(day,shift,pat_dob::date)
                               END AS pat_dob_deid
                              ,shift
                              ,seed
                        FROM dob_shift_cte;`;                        
    //
    stg_qry += `INSERT INTO patid_mapping.patid_xwalk_`+ SITE +` 
                    SELECT patid, patid_hash, pat_dob, pat_dob_deid, shift, seed
                    FROM patid_mapping.patid_xwalk_tmp;`;
}

// execution
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_tmp_qry = snowflake.createStatement({sqlText: new_tmp_qry});
var run_stg_qry = snowflake.createStatement({sqlText: stg_qry});
run_tmp_qry.execute();
run_stg_qry.execute();
commit_txn.execute();
$$
;
