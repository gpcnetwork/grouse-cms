/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: transform_to_diagnosis.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               MEDPAR, INSTITUTIONAL, and NON-INSTITUTIONAL claim into CDM DIAGNOSIS table 
*/

create or replace procedure transform_to_diagnosis(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
// collect columns from target cdm encounter table
var collate_tgt_col = snowflake.createStatement({
    sqlText: `SELECT listagg(column_name,',') as enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = current_schema()
                  AND table_name = 'PRIVATE_DIAGNOSIS';`});
var global_cols = collate_tgt_col.execute(); global_cols.next();
var cols_tgt = global_cols.getColumnValue(1).split(",");

// full-load or cdc-based load
var subset_clause = (SRC_SCHEMA === undefined) ? '': `WHERE a.src_schema = '` + SRC_SCHEMA + `'`;

// step 1 - MEDPAR claims
var t1_qry = `INSERT INTO diagnosis
              SELECT a.bene_id||'|'||a.medparid||'|'||a.dgns_idx||a.dgns_mod
                    ,a.bene_id
                    ,a.medparid
                    ,CASE WHEN a.type_adm = '1' THEN 'EI'
                          WHEN a.sslssnf='N' THEN 'IS' 
                          ELSE 'IP' END
                    ,a.admsndt
                    ,CASE WHEN a.dx_source = 'AD' THEN a.admsndt 
                          WHEN a.sslssnf = 'N' THEN COALESCE(a.cvrlvldt,a.dschrgdt,a.qlfythru)
                          ELSE NVL(a.dschrgdt,a.qlfythru) END
                    ,a.orgnpinm
                    ,CASE WHEN (a.dx_type = '09' OR a.admsndt < '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3+REGEXP_INSTR(a.dx,'E'))||'.'||SUBSTR(a.dx,4+REGEXP_INSTR(a.dx,'E'))
                          WHEN (a.dx_type in ('0','10') OR a.admsndt >= '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3)||'.'||SUBSTR(a.dx,4) 
                          ELSE a.dx END
                    ,CASE WHEN a.admsndt >= '2015-10-01' THEN '10' ELSE '09' END
                    ,a.dx_source
                    ,'CL'
                    ,a.pdx
                    ,CASE WHEN a.dx_poa in ('1','Y','N','U','W') THEN a.dx_poa ELSE 'UN' END
                    ,a.dx
                    ,a.dx_type
                    ,a.dx_source
                    ,a.pdx
                    ,a.dx_poa
              FROM CMS_PCORNET_CDM_STAGING.private_diagnosis_stage_medpar a `+ subset_clause +`;`;
var run_t1_qry = snowflake.createStatement({sqlText: t1_qry});

// step 2 - OUTPATIENT, HHA, HOSPICE claims 
var t2_qry = `INSERT INTO diagnosis
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||a.clm_id||'|'||a.dgns_idx||a.dgns_mod
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.thru_dt)
                     ,a.thru_dt
                     ,a.at_npi
                     ,CASE WHEN (a.dx_type = '9' OR a.from_dt < '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3+REGEXP_INSTR(a.dx,'E'))||'.'||SUBSTR(a.dx,4+REGEXP_INSTR(a.dx,'E'))
                           WHEN (a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3)||'.'||SUBSTR(a.dx,4)
                           ELSE a.dx END
                     ,CASE WHEN a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01' THEN '10' ELSE '09' END
                     ,'FI'
                     ,'CL'
                     ,a.pdx
                     ,a.dx_poa
                     ,a.dx
                     ,a.dx_type
                     ,'FI'
                     ,a.pdx
                     ,a.dx_poa
             FROM CMS_PCORNET_CDM_STAGING.private_diagnosis_stage_outpatient a  
             LEFT JOIN encounter b
             ON a.bene_id = b.patid AND 
                a.thru_dt BETWEEN b.admit_date and b.discharge_date
             `+ subset_clause +`;`;
var run_t2_qry = snowflake.createStatement({sqlText: t2_qry});
                
var t3_qry = `INSERT INTO diagnosis
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||a.clm_id||'|'||a.dgns_idx||a.dgns_mod
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.thru_dt)
                     ,a.thru_dt
                     ,a.at_npi
                     ,CASE WHEN (a.dx_type = '9' OR a.from_dt < '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3+REGEXP_INSTR(a.dx,'E'))||'.'||SUBSTR(a.dx,4+REGEXP_INSTR(a.dx,'E'))
                           WHEN (a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3)||'.'||SUBSTR(a.dx,4)
                           ELSE a.dx END
                     ,CASE WHEN a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01' THEN '10' ELSE '09' END
                     ,'FI'
                     ,'CL'
                     ,a.pdx
                     ,a.dx_poa
                     ,a.dx
                     ,a.dx_type
                     ,'FI'
                     ,a.pdx
                     ,a.dx_poa
             FROM CMS_PCORNET_CDM_STAGING.private_diagnosis_stage_hha a 
             LEFT JOIN encounter b
             ON a.bene_id = b.patid AND 
                a.thru_dt BETWEEN b.admit_date and b.discharge_date
             `+ subset_clause +`;`;
var run_t3_qry = snowflake.createStatement({sqlText: t3_qry});

var t4_qry = `INSERT INTO diagnosis
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||a.clm_id||'|'||a.dgns_idx||a.dgns_mod
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.thru_dt)
                     ,a.thru_dt
                     ,a.at_npi
                     ,CASE WHEN (a.dx_type = '9' OR a.from_dt < '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3+REGEXP_INSTR(a.dx,'E'))||'.'||SUBSTR(a.dx,4+REGEXP_INSTR(a.dx,'E'))
                           WHEN (a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3)||'.'||SUBSTR(a.dx,4)
                           ELSE a.dx END
                     ,CASE WHEN a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01' THEN '10' ELSE '09' END
                     ,'FI'
                     ,'CL'
                     ,a.pdx
                     ,a.dx_poa
                     ,a.dx
                     ,a.dx_type
                     ,'HC:FI'
                     ,a.pdx
                     ,a.dx_poa
             FROM CMS_PCORNET_CDM_STAGING.private_diagnosis_stage_hospice a 
             LEFT JOIN encounter b
             ON a.bene_id = b.patid AND 
                a.thru_dt BETWEEN b.admit_date and b.discharge_date
             `+ subset_clause +`;`;
var run_t4_qry = snowflake.createStatement({sqlText: t4_qry});

// step 3 - BCARRIER, DME claims
var t5_qry = `INSERT INTO diagnosis
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||a.clm_id||'|'||a.dgns_idx||a.dgns_mod
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.thru_dt)
                     ,a.thru_dt
                     ,a.rfr_npi
                     ,CASE WHEN (a.dx_type = '9' OR a.from_dt < '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3+REGEXP_INSTR(a.dx,'E'))||'.'||SUBSTR(a.dx,4+REGEXP_INSTR(a.dx,'E'))
                           WHEN (a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3)||'.'||SUBSTR(a.dx,4)
                           ELSE a.dx END
                     ,CASE WHEN a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01' THEN '10' ELSE '09' END
                     ,'FI'
                     ,'CL'
                     ,a.pdx
                     ,'NI'
                     ,a.dx
                     ,a.dx_type
                     ,'HC:FI'
                     ,a.pdx
                     ,'HC:NI'
             FROM CMS_PCORNET_CDM_STAGING.private_diagnosis_stage_bcarrier a
             LEFT JOIN encounter b
             ON a.bene_id = b.patid AND 
                a.thru_dt BETWEEN b.admit_date and b.discharge_date
             `+ subset_clause +`;`;
var run_t5_qry = snowflake.createStatement({sqlText: t5_qry});

var t6_qry = `INSERT INTO diagnosis
              SELECT  a.bene_id||'|'||NVL(b.encounterid,a.clm_id)||a.clm_id||'|'||a.dgns_idx||a.dgns_mod
                     ,a.bene_id
                     ,NVL(b.encounterid,a.clm_id) 
                     ,NVL(b.enc_type,'NI')
                     ,NVL(b.admit_date,a.thru_dt)
                     ,a.thru_dt
                     ,a.rfr_npi
                     ,CASE WHEN (a.dx_type = '9' OR a.from_dt < '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3+REGEXP_INSTR(a.dx,'E'))||'.'||SUBSTR(a.dx,4+REGEXP_INSTR(a.dx,'E'))
                           WHEN (a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01') AND LEN(a.dx) > 3 THEN SUBSTR(a.dx,1,3)||'.'||SUBSTR(a.dx,4)
                           ELSE a.dx END
                     ,CASE WHEN a.dx_type in ('0','10') OR a.from_dt >= '2015-10-01' THEN '10' ELSE '09' END
                     ,'FI'
                     ,'CL'
                     ,a.pdx
                     ,'NI'
                     ,a.dx
                     ,a.dx_type
                     ,'HC:FI'
                     ,a.pdx
                     ,'HC:NI'
             FROM CMS_PCORNET_CDM_STAGING.private_diagnosis_stage_dme a 
             LEFT JOIN encounter b
             ON a.bene_id = b.patid AND 
                a.thru_dt BETWEEN b.admit_date and b.discharge_date
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

