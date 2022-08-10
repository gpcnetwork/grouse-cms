/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: encounter_stg.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for staging data from 
#               MEDPAR, INSTITUTIONAL, and NON-INSTITUTIONAL claim in preparationg for 
#               CDM ENCOUNTER table transformation
*/

create or replace procedure stage_encounter(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/

// collect columns from target cdm encounter table
var get_tbl_cols = snowflake.createStatement({
    sqlText: `SELECT table_name, listagg(column_name,',') AS enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = 'CMS_PCORNET_CDM_STAGING'
                  AND table_name LIKE 'PRIVATE_ENCOUNTER_STAGE%'
                  AND column_name NOT IN ('SRC_SCHEMA','SRC_TABLE','DEDUP_INDEX')
                GROUP BY table_name;`});
var tables = get_tbl_cols.execute();

// for each staging table
while (tables.next())
{
    var table = tables.getColumnValue(1);
    var cols_var = tables.getColumnValue(2).split(",");
    var cols_raw = cols_var.filter(value =>{return !value.includes('MT_')});
    let stg_pt_qry = '';

    if (table.includes('MEDPAR')) {
        cols_raw = cols_raw.map(value =>{return 'mpa.' + value});
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols_var +`,src_schema,src_table,dedup_index)
                       WITH cmap as (
                        SELECT `+ cols_raw +`
                              ,CASE WHEN mpa.type_adm = '1' THEN 'EI'
                                    WHEN mpa.sslssnf='N' THEN 'IS' 
                                    ELSE 'IP' END AS mt_enc_type
                              ,CASE WHEN mpa.sslssnf='N' THEN COALESCE(mpa.cvrlvldt,mpa.dschrgdt,mpa.qlfythru)
                                    ELSE NVL(mpa.dschrgdt,mpa.QLFYTHRU) END AS mt_discharge_date
                              ,CASE WHEN p2f.FACILITY_TYPE is not null THEN p2f.FACILITY_TYPE
                                    WHEN mpa.sslssnf='N' THEN 'SKILLED_NURSING_FACILITY'
                                    ELSE 'HOSPITAL_COMMUNITY' END AS mt_facility_type
                              ,CASE WHEN mpa.DSCHRGCD = 'A' THEN 'A'
                                    WHEN mpa.DSCHRGCD = 'B' THEN 'E'
                                    WHEN mpa.DSCHRGCD = 'C' THEN 'OT'
                                    ELSE 'NI' END AS mt_discharge_disposition
                              ,COALESCE(st2st.DISCHARGE_STATUS,'NI') AS mt_discharge_status
                              ,coalesce(sr2sr.ADMITTING_SOURCE,'NI') AS mt_admitting_source
                              ,coalesce(p2p.PAYER_TYPE_PRIMARY,'NI') AS mt_payer_type_primary
                              ,coalesce(p2p.PAYER_TYPE_SECONDARY,'NI') AS mt_payer_type_secondary
                        FROM ` + SRC_SCHEMA + `.MEDPAR_ALL mpa
                        LEFT JOIN CONCEPT_MAPPINGS.PRVDNUM2FACTYPE p2f on TRY_TO_NUMERIC(RIGHT(mpa.PRVDRNUM,4)) between p2f.PRVDRNUM_LB and p2f.PRVDRNUM_UB
                        LEFT JOIN CONCEPT_MAPPINGS.STUS2STUS st2st on mpa.DSTNTNCD = st2st.STUSCD
                        LEFT JOIN CONCEPT_MAPPINGS.SRCADMS2SRCADMS sr2sr on mpa.SRC_ADMS = sr2sr.SRCADMS
                        LEFT JOIN CONCEPT_MAPPINGS.PRPAY2PAYER p2p on mpa.PRPAY_CD = p2p.PRPAYCD
                       )
                       SELECT DISTINCT `+ cols_var +`, '` + SRC_SCHEMA + `','MEDPAR_ALL',
                              ROW_NUMBER() OVER (PARTITION BY bene_id, mt_enc_type, admsndt ORDER BY prpay_cd, mt_discharge_date desc) dedup_index
                       FROM cmap;`;
                        
    } else if (table.includes('OUTPATIENT')) {
        cols_raw = cols_raw.map(value =>{return 'op.' + value});
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols_var +`,src_schema,src_table,dedup_index)
                       WITH ed_rev_cntr AS (
                       SELECT a.*, max(a.thru_dt) OVER (PARTITION BY a.bene_id, a.series) thru_dt_last,
                              row_number() OVER (PARTITION BY a.bene_id, a.series ORDER BY a.thru_dt) rn
                       FROM (SELECT bene_id, clm_id, thru_dt, rev_cntr,
                                    DATEADD(day, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY thru_dt)+1,thru_dt) AS series
                             FROM `+SRC_SCHEMA+`.OUTPATIENT_REVENUE_CENTER
                             WHERE rev_cntr IN ('0450','0451','0452','0453','0454','0455','0456','0457','0458','0459','0981')) a
                       ),   os_rev_cntr as (
                       SELECT b.*, max(b.thru_dt) OVER (PARTITION BY b.bene_id, b.series) thru_dt_last,
                              row_number() OVER (PARTITION BY b.bene_id, b.series ORDER BY b.thru_dt) rn
                       FROM (SELECT bene_id, clm_id, thru_dt, rev_cntr,
                                    DATEADD(day, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY thru_dt)+1,thru_dt) AS series
                             FROM `+SRC_SCHEMA+`.OUTPATIENT_REVENUE_CENTER
                             WHERE rev_cntr IN ('0760','0761','0762','0769')) b
                       ),   th_rev_cntr as (
                       SELECT c.*, max(c.thru_dt) OVER (PARTITION BY c.bene_id, c.series) thru_dt_last, 
                              row_number() OVER (PARTITION BY c.bene_id, c.series ORDER BY c.thru_dt) rn
                       FROM (SELECT bene_id, clm_id, thru_dt, rev_cntr,
                                    DATEADD(day, - DENSE_RANK() OVER (PARTITION BY bene_id ORDER BY thru_dt)+1,thru_dt) AS series
                             FROM `+SRC_SCHEMA+`.OUTPATIENT_REVENUE_CENTER
                             WHERE rev_cntr IN ('0780','0789')) c
                       ), cmap as (
                       SELECT `+ cols_raw +`
                             ,CASE WHEN ed.thru_dt_last is not null THEN 'ED'
                                   WHEN os.thru_dt_last is not null THEN 'OS'
                                   WHEN th.thru_dt_last is not null THEN 'TH'
                                   WHEN op.fac_type in ('1','7','8') THEN 'AV'
                                   WHEN op.fac_type in ('2','3') THEN 'OA'
                                   ELSE 'OT' END AS mt_enc_type
                             ,CASE WHEN ed.thru_dt is not null THEN ed.THRU_DT
                                   WHEN os.thru_dt is not null THEN os.THRU_DT
                                   WHEN th.thru_dt is not null THEN th.THRU_DT
                                   ELSE op.thru_dt END AS mt_discharge_date
                             ,CASE WHEN ed.thru_dt is not null THEN 'EMERGENCY_DEPARTMENT_HOSPITAL'
                                   ELSE coalesce(t2f.FACILITY_TYPE, p2f.FACILITY_TYPE, 'NI') END AS mt_facility_type
                             ,COALESCE(st2st.DISCHARGE_STATUS,'NI') AS mt_discharge_status
                             ,COALESCE(p2p.PAYER_TYPE_PRIMARY,'NI') AS mt_payer_type_primary
                             ,COALESCE(p2p.PAYER_TYPE_SECONDARY,'NI') AS mt_payer_type_secondary
                       FROM `+SRC_SCHEMA+`.OUTPATIENT_BASE_CLAIMS op
                       LEFT JOIN ed_rev_cntr ed ON op.BENE_ID = ed.BENE_ID AND op.CLM_ID = ed.CLM_ID AND ed.rn = 1
                       LEFT JOIN os_rev_cntr os ON op.BENE_ID = os.BENE_ID AND op.CLM_ID = os.CLM_ID AND os.rn = 1
                       LEFT JOIN th_rev_cntr th ON op.BENE_ID = th.BENE_ID AND op.CLM_ID = th.CLM_ID AND th.rn = 1
                       LEFT JOIN CONCEPT_MAPPINGS.TOB2FACTYPE t2f ON (op.FAC_TYPE||op.TYPESRVC) = t2f.TOB 
                       LEFT JOIN CONCEPT_MAPPINGS.PRVDNUM2FACTYPE p2f ON TRY_TO_NUMERIC(op.PROVIDER) BETWEEN p2f.PRVDRNUM_LB AND p2f.PRVDRNUM_UB
                       LEFT JOIN CONCEPT_MAPPINGS.STUS2STUS st2st ON op.STUS_CD = st2st.STUSCD
                       LEFT JOIN CONCEPT_MAPPINGS.PRPAY2PAYER p2p ON op.PRPAY_CD = p2p.PRPAYCD
                       )
                       SELECT DISTINCT `+ cols_var +`,'` + SRC_SCHEMA + `','OUTPATIENT_BASE_CLAIMS',
                              ROW_NUMBER() OVER (PARTITION BY bene_id, mt_enc_type, from_dt ORDER BY prpay_cd, mt_discharge_date desc) dedup_index
                       FROM cmap;`;
                                    
    } else if (table.includes('HHA') || table.includes('HOSPICE')) {
        var tbl_prefix = table.includes('HHA') ? 'HHA' : 'HOSPICE';
        cols_raw = cols_raw.map(value =>{return 'inst.' + value});
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols_var +`,src_schema,src_table,dedup_index)
                       WITH cmap AS (
                        SELECT `+ cols_raw +`
                              ,CASE WHEN inst.clm_type = '50' THEN 'IS'
                                    WHEN inst.clm_type = '10' THEN 'OA'
                                    ELSE 'OT' END AS mt_enc_type
                              ,COALESCE(t2f.FACILITY_TYPE, p2f.FACILITY_TYPE, 'NI') AS mt_facility_type
                              ,COALESCE(st2st.DISCHARGE_STATUS,'NI') AS mt_discharge_status
                              ,COALESCE(p2p.PAYER_TYPE_PRIMARY,'NI') AS mt_payer_type_primary
                              ,COALESCE(p2p.PAYER_TYPE_SECONDARY,'NI') AS mt_payer_type_secondary
                        FROM ` + SRC_SCHEMA + `.`+ tbl_prefix +`_BASE_CLAIMS inst
                        LEFT JOIN CONCEPT_MAPPINGS.TOB2FACTYPE t2f ON (inst.FAC_TYPE||inst.TYPESRVC) = t2f.TOB 
                        LEFT JOIN CONCEPT_MAPPINGS.PRVDNUM2FACTYPE p2f ON TRY_TO_NUMERIC(inst.PROVIDER) BETWEEN p2f.PRVDRNUM_LB AND p2f.PRVDRNUM_UB
                        LEFT JOIN CONCEPT_MAPPINGS.STUS2STUS st2st ON inst.STUS_CD = st2st.STUSCD
                        LEFT JOIN CONCEPT_MAPPINGS.PRPAY2PAYER p2p ON inst.PRPAY_CD = p2p.PRPAYCD
                       )
                       SELECT DISTINCT `+ cols_var +`,'` + SRC_SCHEMA + `','`+ tbl_prefix +`_BASE_CLAIMS',
                              ROW_NUMBER() OVER (PARTITION BY bene_id, mt_enc_type, from_dt ORDER BY prpay_cd, thru_dt DESC) dedup_index
                       FROM cmap;`;
                        
    } else if (table.includes('BCARRIER') || table.includes('DME')) {
        var tbl_prefix = table.includes('BCARRIER') ? 'BCARRIER' : 'DME';
        cols_line = cols_raw.filter(value => {return !value.includes('RFR_NPI') && !value.includes('ORG_NPI')}).map(value =>{return 'line.' + value});
        cols_clm = cols_raw.filter(value => {return value.includes('RFR_NPI') || value.includes('ORG_NPI')}).map(value =>{return 'clm.' + value});
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols_var +`,src_schema,src_table,dedup_index)
                       WITH cmap AS (
                        SELECT `+ cols_line +`,`+ cols_clm +`, clm.from_dt
                              ,COALESCE(p2e.ENC_TYPE,'NI') AS mt_enc_type
                              ,COALESCE(p2f.FACILITY_TYPE, 'NI') AS mt_facility_type
                              ,COALESCE(p2p.PAYER_TYPE_PRIMARY,'NI') AS mt_payer_type_primary
                              ,COALESCE(p2p.PAYER_TYPE_SECONDARY,'NI') AS mt_payer_type_secondary
                        FROM ` + SRC_SCHEMA + `.`+ tbl_prefix +`_LINE line
                        JOIN ` + SRC_SCHEMA + `.`+ tbl_prefix +`_CLAIMS clm on line.BENE_ID = clm.BENE_ID AND line.CLM_ID = clm.CLM_ID
                        LEFT JOIN CONCEPT_MAPPINGS.POS2ENCTYPE p2e on line.PLCSRVC = p2e.POS
                        LEFT JOIN CONCEPT_MAPPINGS.POS2FACTYPE p2f on line.PLCSRVC = p2f.POS
                        LEFT JOIN CONCEPT_MAPPINGS.PRPAY2PAYER p2p on line.LPRPAYCD = p2p.PRPAYCD
                       )
                       SELECT DISTINCT `+ cols_var +`,'` + SRC_SCHEMA + `','`+ tbl_prefix +`_LINE',
                              ROW_NUMBER() OVER (PARTITION BY bene_id, mt_enc_type, from_dt ORDER BY lprpaycd, thru_dt DESC) dedup_index
                       FROM cmap;`;
    } else {
        continue;
    }
    /**
    // preview of the generated dynamic SQL scripts - comment it out when perform actual execution
       var log_stmt = snowflake.createStatement({
                        sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                        binds: [stg_pt_qry]});
       log_stmt.execute(); 
    **/
    // run dynamic dml query
    var add_new = snowflake.createStatement({sqlText: stg_pt_qry});
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    add_new.execute();
    commit_txn.execute();
}            
$$
;

