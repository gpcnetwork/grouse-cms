/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: encounter_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               MEDPAR, INSTITUTIONAL, and NON-INSTITUTIONAL claim into CDM ENCOUNTER table 
*/
create or replace procedure transform_to_encounter(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/

// collect columns from target cdm encounter table
var collate_col_stmt = snowflake.createStatement({
    sqlText: `SELECT listagg(column_name,',') as enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = current_schema()
                  AND table_name = 'PRIVATE_ENCOUNTER';`});
var global_cols = collate_col_stmt.execute(); global_cols.next();
var cols_tgt = global_cols.getColumnValue(1).split(",");
var cols_tgt_mod = cols_tgt.map(item => {return 's.' + item});

// full-load or cdc-based load
var subset_clause = (SRC_SCHEMA === undefined) ? '': `WHERE src_schema = '` + SRC_SCHEMA + `'`;

// step 1 - MEDPAR (curation in source)
var t1_qry = `INSERT INTO private_encounter
              SELECT bene_id 
                    ,medparid
                    ,mt_enc_type
                    ,admsndt
                    ,mt_discharge_date
                    ,orgnpinm
                    ,prvdrnum
                    ,mt_facility_type
                    ,mt_discharge_disposition
                    ,mt_discharge_status
                    ,drg_cd
                    ,'02'
                    ,mt_admitting_source
                    ,mt_payer_type_primary
                    ,mt_payer_type_secondary
                    ,type_adm || '|' || sslssnf
                    ,dschrgcd
                    ,dstntncd
                    ,'HC:02'
                    ,src_adms
                    ,RIGHT(prvdrnum,4)
                    ,prvdrnum
                    ,prpay_cd
              FROM CMS_PCORNET_CDM_STAGING.private_encounter_stage_medpar `+ subset_clause + `
                   AND dedup_index = 1;`;
var run_t1_qry = snowflake.createStatement({sqlText: t1_qry});

// step 2 - OUTPATIENT claims (curation at staging)
var t2_qry = `MERGE INTO private_encounter t
              USING (
                SELECT bene_id AS patid
                      ,clm_id AS encounterid
                      ,mt_enc_type AS enc_type
                      ,from_dt AS admit_date
                      ,mt_discharge_date AS discharge_date
                      ,at_npi AS providerid
                      ,provider AS facilityid
                      ,mt_facility_type AS facility_type
                      ,'NI' AS discharge_disposition
                      ,mt_discharge_status AS discharge_status
                      ,'NI' AS drg
                      ,'NI' AS drg_type
                      ,'NI' AS admitting_source
                      ,mt_payer_type_primary AS payer_type_primary
                      ,mt_payer_type_secondary AS payer_type_secondary
                      ,fac_type AS raw_enc_type
                      ,'HC:NI' AS raw_discharge_disposition
                      ,stus_cd AS raw_discharge_status
                      ,'HC:NI' AS raw_drg_type
                      ,'HC:NI' AS raw_admitting_source
                      ,RIGHT(provider,4) AS raw_facility_type
                      ,provider AS raw_facility_code
                      ,prpay_cd AS raw_payer_type_primary
                FROM CMS_PCORNET_CDM_STAGING.private_encounter_stage_outpatient `+ subset_clause + `
                     AND dedup_index = 1
              ) s
              -- encounter consolidation --
              ON t.patid = s.patid AND s.admit_date BETWEEN DATEADD(day, -1, t.admit_date) AND t.discharge_date 
              WHEN MATCHED AND t.enc_type = 'EI' AND s.enc_type = 'ED' AND s.discharge_date = t.admit_date
                   THEN UPDATE SET t.admit_date = s.admit_date                      
              WHEN NOT MATCHED
                   THEN INSERT(` + cols_tgt + `) VALUES (` + cols_tgt_mod + `);`;
var run_t2_qry = snowflake.createStatement({sqlText: t2_qry});

// step 3 - HHA, HOSPICE claims
var t3_qry = `INSERT INTO private_encounter
              SELECT bene_id
                    ,clm_id
                    ,mt_enc_type
                    ,from_dt
                    ,thru_dt
                    ,at_npi
                    ,provider
                    ,mt_facility_type
                    ,'NI' AS discharge_disposition
                    ,mt_discharge_status
                    ,'NI' AS drg
                    ,'NI' AS drg_type
                    ,'NI' AS admitting_source
                    ,mt_payer_type_primary
                    ,mt_payer_type_secondary
                    ,clm_type
                    ,'HC:NI' AS raw_discharge_disposition
                    ,stus_cd
                    ,'HC:NI' AS raw_drg_type
                    ,'HC:NI' AS raw_admitting_source
                    ,RIGHT(provider,4)
                    ,provider
                    ,prpay_cd
              FROM CMS_PCORNET_CDM_STAGING.private_encounter_stage_hha `+ subset_clause + `
                   AND dedup_index = 1;`;
var run_t3_qry = snowflake.createStatement({sqlText: t3_qry});

var t4_qry = `INSERT INTO private_encounter
              SELECT bene_id
                    ,clm_id
                    ,mt_enc_type
                    ,from_dt
                    ,thru_dt
                    ,at_npi
                    ,provider
                    ,mt_facility_type
                    ,'NI' AS discharge_disposition
                    ,mt_discharge_status
                    ,'NI' AS drg
                    ,'NI' AS drg_type
                    ,'NI' AS admitting_source
                    ,mt_payer_type_primary
                    ,mt_payer_type_secondary
                    ,clm_type
                    ,'HC:NI' AS raw_discharge_disposition
                    ,stus_cd
                    ,'HC:NI' AS raw_drg_type
                    ,'HC:NI' AS raw_admitting_source
                    ,RIGHT(provider,4)
                    ,provider
                    ,prpay_cd
              FROM CMS_PCORNET_CDM_STAGING.private_encounter_stage_hospice `+ subset_clause + `
                   AND dedup_index = 1;`;
var run_t4_qry = snowflake.createStatement({sqlText: t4_qry});
  
// step 4 - BCARRIER, DME claims (consolidation)
var t5_qry = `MERGE INTO private_encounter t
              USING (
                SELECT bene_id AS patid
                      ,clm_id AS encounterid
                      ,mt_enc_type AS enc_type
                      ,thru_dt AS admit_date
                      ,thru_dt AS discharge_date
                      ,prf_npi AS providerid
                      ,plcsrvc AS facilityid
                      ,mt_facility_type AS facility_type
                      ,'NI' AS discharge_disposition
                      ,'NI' AS discharge_status
                      ,'NI' AS drg
                      ,'NI' AS drg_type
                      ,'NI' AS admitting_source
                      ,mt_payer_type_primary AS payer_type_primary
                      ,mt_payer_type_secondary AS payer_type_secondary
                      ,plcsrvc AS raw_enc_type
                      ,'HC:NI' AS raw_discharge_disposition
                      ,'HC:NI' AS raw_discharge_status
                      ,'HC:NI' AS raw_drg_type
                      ,'HC:NI' AS raw_admitting_source
                      ,plcsrvc AS raw_facility_type
                      ,plcsrvc AS raw_facility_code
                      ,NVL(lprpaycd,mt_payer_type_primary) AS raw_payer_type_primary
                FROM CMS_PCORNET_CDM_STAGING.private_encounter_stage_bcarrier `+ subset_clause + `
                     AND dedup_index = 1
              ) s
              -- encounter consolidation --
              ON t.patid = s.patid AND s.admit_date between t.admit_date and t.discharge_date                      
              WHEN NOT MATCHED
                   THEN INSERT(` + cols_tgt + `) VALUES (` + cols_tgt_mod + `);`;
var run_t5_qry = snowflake.createStatement({sqlText: t5_qry});
                        
var t6_qry = `MERGE INTO private_encounter t
              USING (
                SELECT bene_id AS patid
                      ,clm_id AS encounterid
                      ,mt_enc_type AS enc_type
                      ,thru_dt AS admit_date
                      ,thru_dt AS discharge_date
                      ,sup_npi AS providerid
                      ,plcsrvc AS facilityid
                      ,mt_facility_type AS facility_type
                      ,'NI' AS discharge_disposition
                      ,'NI' AS discharge_status
                      ,'NI' AS drg
                      ,'NI' AS drg_type
                      ,'NI' AS admitting_source
                      ,mt_payer_type_primary AS payer_type_primary
                      ,mt_payer_type_secondary AS payer_type_secondary
                      ,plcsrvc AS raw_enc_type
                      ,'HC:NI' AS raw_discharge_disposition
                      ,'HC:NI' AS raw_discharge_status
                      ,'HC:NI' AS raw_drg_type
                      ,'HC:NI' AS raw_admitting_source
                      ,plcsrvc AS raw_facility_type
                      ,plcsrvc AS raw_facility_code
                      ,NVL(lprpaycd,mt_payer_type_primary) AS raw_payer_type_primary
                FROM CMS_PCORNET_CDM_STAGING.private_encounter_stage_dme `+ subset_clause + `
                     AND dedup_index = 1
              ) s
              -- encounter consolidation --
              ON t.patid = s.patid AND s.admit_date between t.admit_date and t.discharge_date                      
              WHEN NOT MATCHED
                   THEN INSERT(` + cols_tgt + `) VALUES (` + cols_tgt_mod + `);`;
var run_t6_qry = snowflake.createStatement({sqlText: t6_qry});

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

