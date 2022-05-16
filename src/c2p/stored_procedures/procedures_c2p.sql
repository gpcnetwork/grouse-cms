/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: procedures_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               MEDPAR, INSTITUTIONAL, and NON-INSTITUTIONAL claim into CDM PROCEDURES table 
*/
create or replace procedure transform_to_procedures(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/

// collect columns from target cdm encounter table
var collate_tgt_col = snowflake.createStatement({
    sqlText: `SELECT listagg(column_name,',') as enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = current_schema()
                  AND table_name = 'PRIVATE_PROCEDURES';`});
var global_cols = collate_tgt_col.execute(); global_cols.next();
var cols_tgt = global_cols.getColumnValue(1).split(",");

// full-load or cdc-based load
var subset_clause = (SRC_SCHEMA === undefined) ? '': `WHERE a.src_schema = '` + SRC_SCHEMA + `'`;

// step 1 - MEDPAR claims
var t1_qry = `INSERT INTO private_procedures
              SELECT a.bene_id||'|'||a.medparid||'|'||to_number(a.px_idx)
                    ,a.bene_id
                    ,a.medparid
                    ,CASE WHEN a.type_adm = '1' THEN 'EI' WHEN a.sslssnf='N' THEN 'IS' ELSE 'IP' END
                    ,a.admsndt
                    ,a.orgnpinm
                    ,a.px_date
                    ,CASE WHEN a.px_type = '09' THEN SUBSTR(a.px,1,2)||'.'||SUBSTR(a.px,3) ELSE a.px END
                    ,CASE WHEN a.px_date >= '2015-10-01' THEN NVL(a.px_type,'10') ELSE NVL(a.px_type,'09') END
                    ,'CL'
                    ,a.ppx
                    ,a.px
                    ,a.px_type
                    ,a.ppx
              FROM CMS_PCORNET_CDM_STAGING.private_procedures_stage_medpar a `+ subset_clause +`;`;
var run_t1_qry = snowflake.createStatement({sqlText: t1_qry});

// step 2 - OUTPATIENT claims 
var t2_qry = `INSERT INTO private_procedures
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||'|'||a.clm_id||'|'||to_number(a.px_idx)
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.px_date)
                     ,a.provider_npi
                     ,a.px_date
                     ,CASE WHEN a.px_type = '09' THEN SUBSTR(a.px,1,2)||'.'||SUBSTR(a.px,3) ELSE a.px END
                     ,CASE WHEN a.px_date >= '2015-10-01' THEN NVL(a.px_type,'10') ELSE NVL(a.px_type,'09') END
                     ,'CL'
                     ,a.ppx
                     ,a.px
                     ,a.px_type
                     ,a.ppx
             FROM CMS_PCORNET_CDM_STAGING.private_procedures_stage_outpatient a  
             LEFT JOIN private_encounter b
             ON a.bene_id = b.patid AND a.px_date BETWEEN b.admit_date and b.discharge_date
                `+ subset_clause +`;`;
var run_t2_qry = snowflake.createStatement({sqlText: t2_qry});

// step 3 - HHA, HOSPICE claims
var t3_qry = `INSERT INTO private_procedures
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||'|'||a.clm_id||'|'||to_number(a.px_idx)
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.px_date)
                     ,a.provider_npi
                     ,a.px_date
                     ,a.px
                     ,a.px_type
                     ,'CL'
                     ,'NI'
                     ,a.px
                     ,a.px_type
                     ,'HC:NI'
             FROM CMS_PCORNET_CDM_STAGING.private_procedures_stage_hha a 
             LEFT JOIN private_encounter b
             ON a.bene_id = b.patid AND a.px_date BETWEEN b.admit_date and b.discharge_date
                `+ subset_clause +`;`;
var run_t3_qry = snowflake.createStatement({sqlText: t3_qry});

var t4_qry = `INSERT INTO private_procedures
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||'|'||a.clm_id||'|'||to_number(a.px_idx)
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.px_date)
                     ,a.provider_npi
                     ,a.px_date
                     ,a.px
                     ,a.px_type
                     ,'CL'
                     ,'NI'
                     ,a.px
                     ,a.px_type
                     ,'HC:NI'
             FROM CMS_PCORNET_CDM_STAGING.private_procedures_stage_hospice a 
             LEFT JOIN private_encounter b
             ON a.bene_id = b.patid AND a.px_date BETWEEN b.admit_date and b.discharge_date
                `+ subset_clause +`;`;
var run_t4_qry = snowflake.createStatement({sqlText: t4_qry});

// step 4 - BCARRIER, DME claims
var t5_qry = `INSERT INTO private_procedures
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||'|'||a.clm_id||'|'||to_number(a.px_idx)
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.px_date)
                     ,a.provider_npi
                     ,a.px_date
                     ,a.px
                     ,a.px_type
                     ,'CL'
                     ,'NI'
                     ,a.px
                     ,a.px_type
                     ,'HC:NI'
             FROM CMS_PCORNET_CDM_STAGING.private_procedures_stage_bcarrier a
             LEFT JOIN private_encounter b
             ON a.bene_id = b.patid AND a.px_date BETWEEN b.admit_date and b.discharge_date
                `+ subset_clause +`;`;
var run_t5_qry = snowflake.createStatement({sqlText: t5_qry});

var t6_qry = `INSERT INTO private_procedures
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||'|'||a.clm_id||'|'||to_number(a.px_idx)
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.px_date)
                     ,a.provider_npi
                     ,a.px_date
                     ,a.px
                     ,a.px_type
                     ,'CL'
                     ,'NI'
                     ,a.px
                     ,a.px_type
                     ,'HC:NI'
             FROM CMS_PCORNET_CDM_STAGING.private_procedures_stage_dme a 
             LEFT JOIN private_encounter b
             ON a.bene_id = b.patid AND a.px_date BETWEEN b.admit_date and b.discharge_date
             `+ subset_clause +`;`;
var run_t6_qry = snowflake.createStatement({sqlText: t6_qry});

/**
// preview of the generated dynamic SQL scripts - comment it out when perform actual execution
var log_stmt = snowflake.createStatement({
                sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                binds: [run_t1_qry]});
log_stmt.execute(); 
**/

// run dynamic dml queries
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
run_t1_qry.execute();
run_t2_qry.execute();
run_t3_qry.execute();
run_t4_qry.execute();
run_t5_qry.execute();
run_t6_qry.execute();
commit_txn.execute();
$$
;

