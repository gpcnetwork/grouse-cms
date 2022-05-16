/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: diagnosis_stg.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for staging data from 
#               MEDPAR, INSTITUTIONAL, and NON-INSTITUTIONAL claim in preparationg for 
#               CDM DIAGNOSIS table transformation
*/
create or replace procedure stage_diagnosis(SRC_SCHEMA STRING, PART STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
 * @param {string} PART: part name of source table
**/

// collect columns from target staging table
var get_tbl_cols = snowflake.createStatement({
    sqlText: `SELECT table_name, listagg(column_name,',') AS enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = 'CMS_PCORNET_CDM_STAGING'
                  AND table_name LIKE 'PRIVATE_DIAGNOSIS_STAGE%'
                  AND column_name NOT IN ('SRC_SCHEMA','SRC_TABLE')
                GROUP BY table_name;`});
var tables = get_tbl_cols.execute();

// for each staging table
while (tables.next())
{
    // parameters for target table
    var table = tables.getColumnValue(1);
    var cols = tables.getColumnValue(2).split(",");
    let stg_pt_qry = '';
       
    if(table.includes('MEDPAR') && (table.include(PART)||(PART===undefined))){
        // parameters for source table
        var collate_col_stmt = `SELECT table_name, listagg(column_name,',') as cols 
                                FROM information_schema.columns 
                                WHERE table_catalog = 'GROUSE_DB' 
                                    AND table_name like 'MEDPAR%' 
                                    AND table_schema = '`+ SRC_SCHEMA +`' 
                                    AND ((column_name like '%DGNS%CD%' AND column_name NOT like '%CNT') OR column_name in ('AD_DGNS'))
                                GROUP BY table_name;`
        var get_cols_raw_stmt = snowflake.createStatement({sqlText: collate_col_stmt});
        var get_cols_raw = get_cols_raw_stmt.execute(); get_cols_raw.next();
        var cols_raw = get_cols_raw.getColumnValue(2).split(",");
        var cols_dx = cols_raw.filter(value => {return value.includes('DGNS') && !value.includes('VRSN') && !value.includes('AD') && !value.includes('POA')});
        var cols_dxtype = cols_raw.filter(value => {return value.includes('DGNS') && value.includes('VRSN') && !value.includes('AD')});
        var cols_dxpoa = cols_raw.filter(value => {return value.includes('POA_DGNS')});
        var cols_ad_dx = cols_raw.filter(value => {return value.includes('AD') && value.includes('DGNS') && !value.includes('VRSN')});
        var cols_ad_dxtype = cols_raw.filter(value => {return value.includes('AD') && value.includes('DGNS') && value.includes('VRSN')});
    
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                       WITH cte_unpvt as (
                        -- admitting diagnosis 
                        SELECT bene_id, medparid, type_adm, sslssnf, admsndt, dschrgdt, cvrlvldt, qlfythru, 
                               orgnpinm, `+ cols_ad_dx + ` AS dx, `+ cols_ad_dxtype +` AS dx_type, 'Y' AS dx_poa, 'AD' as dx_source,
                               '' AS dgns_idx, '' AS dgns_mod, 'NI' as pdx
                        FROM (SELECT * FROM `+ SRC_SCHEMA +`.MEDPAR_ALL)
                        UNION
                        -- collect all the other diagnoses besides admitting diagnosis
                        SELECT bene_id, medparid, type_adm, sslssnf, admsndt, dschrgdt, cvrlvldt, qlfythru,
                               orgnpinm, dx, dx_type, dx_poa, 'DI' as dx_source,
                               TRIM(REGEXP_REPLACE(dx_idx, '[^[:digit:]]', ' ')) AS dgns_idx,
                               CASE WHEN dx_idx LIKE '%_E_%' THEN 'E' ELSE '' END AS dgns_mod,
                               CASE WHEN TRIM(REGEXP_REPLACE(dx_idx, '[^[:digit:]]', ' ')) = '1' THEN 'P' ELSE 'S' END AS pdx
                        FROM (SELECT * FROM `+ SRC_SCHEMA +`.MEDPAR_ALL)
                        -- multi unpivot
                        UNPIVOT (dx for dx_idx in (` + cols_dx + `)) dx_unpvt
                        UNPIVOT (dx_type for dx_type_idx in (` + cols_dxtype + `)) dxtype_unpvt    
                        UNPIVOT (dx_poa for dx_poa_idx in (` + cols_dxpoa + `)) dxpoa_unpvt
                        WHERE REGEXP_REPLACE(dx_idx,'[[:alpha:]]|_') = REGEXP_REPLACE(dx_type_idx,'[[:alpha:]]|_') AND
                              REGEXP_SUBSTR(dx_idx,'_E_') = REGEXP_SUBSTR(dx_type_idx,'_E_') AND
                              REGEXP_REPLACE(dx_idx,'[[:alpha:]]|_') = REGEXP_REPLACE(dx_poa_idx,'[[:alpha:]]|_') AND
                              REGEXP_SUBSTR(dx_idx,'_E_') = REGEXP_SUBSTR(dx_poa_idx,'_E_') AND
                              TRIM(dx) is not null AND TRIM(dx) <> ''
                       )
                       SELECT `+ cols +`, '` + SRC_SCHEMA + `','MEDPAR_ALL'
                       FROM cte_unpvt;`;
                       
    } else if((table.includes('OUTPATIENT') || table.includes('HHA') || table.includes('HOSPICE')) && (table.include(PART)||(PART===undefined))){
        var tbl_prefix = table.includes('OUTPATIENT') ? 'OUTPATIENT' : (table.includes('HHA') ? 'HHA' : 'HOSPICE');
        
        // parameters for source table
        var collate_col_stmt = `SELECT table_name, listagg(column_name,',') as cols 
                                FROM information_schema.columns 
                                WHERE table_catalog = 'GROUSE_DB' 
                                    AND table_name = '`+ tbl_prefix +`_BASE_CLAIMS' 
                                    AND table_schema = '`+ SRC_SCHEMA +`' 
                                    AND ((column_name like '%DGNS%CD%' AND column_name NOT like 'PRNCPAL%' AND column_name NOT like 'FST%') OR
                       column_name like '%RSN_VISIT%')
                                GROUP BY table_name;`
        var get_cols_raw_stmt = snowflake.createStatement({sqlText: collate_col_stmt});
        var get_cols_raw = get_cols_raw_stmt.execute(); get_cols_raw.next();
        var cols_raw = get_cols_raw.getColumnValue(2).split(",");
        var cols_dx = cols_raw.filter(value => {return (value.includes('DGNS') || value.includes('RSN_VISIT')) && !value.includes('VRSN')});
        var cols_dxtype = cols_raw.filter(value => {return (value.includes('DGNS') || value.includes('RSN_VISIT')) && value.includes('VRSN')});
        
        // no icd version columns in medicare data after version K (>=2014)
        var dxtype_phrase = [];
        if (cols_dxtype === undefined || cols_dxtype.length == 0) {
            dxtype_phrase.push('null AS dx_type');
            dxtype_phrase.push('');
            dxtype_phrase.push(`WHERE TRIM(dx) is not null AND TRIM(dx) <> ''`);
        } else {
            dxtype_phrase.push('dx_type');
            dxtype_phrase.push(`UNPIVOT (dx_type for dx_type_idx in (` + cols_dxtype + `)) dxtype_unpvt`);
            dxtype_phrase.push(`WHERE RIGHT(dx_idx,4) = RIGHT(dx_type_idx,4) AND LEFT(dx_idx,10) = LEFT(dx_type_idx,10) AND TRIM(dx) is not null AND TRIM(dx) <> ''`);
        }
    
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                       WITH cte_unpvt as (
                        SELECT  bene_id, clm_id, from_dt, thru_dt, at_npi,
                                TRIM(REGEXP_REPLACE(dx_idx, '[^[:digit:]]', ' ')) AS dgns_idx,
                                CASE WHEN dx_idx LIKE '%_E_%' THEN 'E' ELSE '' END AS dgns_mod,
                                dx, ` + dxtype_phrase[0] + `, 
                                CASE WHEN dx LIKE 'RSN%' THEN 'Y' ELSE 'UN' END AS dx_poa,
                                CASE WHEN TRIM(REGEXP_REPLACE(dx_type, '[^[:digit:]]', ' ')) = '1' THEN 'P' ELSE 'S' END AS pdx
                        FROM (SELECT * FROM ` + SRC_SCHEMA + `.`+ tbl_prefix +`_BASE_CLAIMS)
                        -- multi unpivot
                        UNPIVOT (dx for dx_idx in (` + cols_dx + `)) dx_unpvt
                                `+ dxtype_phrase[1] +`
                                `+ dxtype_phrase[2] +` 
                      )
                      SELECT `+ cols +`, '` + SRC_SCHEMA + `','`+ tbl_prefix +`_BASE_CLAIMS'
                      FROM cte_unpvt;`;
        
    } else if((table.includes('BCARRIER') || table.includes('DME')) && (table.include(PART)||(PART===undefined))){
        var tbl_prefix = table.includes('BCARRIER') ? 'BCARRIER' : 'DME';
        
        // parameters for source table
        var collate_col_stmt = `SELECT table_name, listagg(column_name,',') as cols 
                                FROM information_schema.columns 
                                WHERE table_catalog = 'GROUSE_DB' 
                                    AND table_name = '`+ tbl_prefix +`_CLAIMS' 
                                    AND table_schema = '`+ SRC_SCHEMA +`' 
                                    AND column_name like '%DGNS%CD%' AND column_name NOT like 'PRNCPAL%' AND column_name NOT like 'FST%'
                                GROUP BY table_name;`
        var get_cols_raw_stmt = snowflake.createStatement({sqlText: collate_col_stmt});
        var get_cols_raw = get_cols_raw_stmt.execute(); get_cols_raw.next();
        var cols_raw = get_cols_raw.getColumnValue(2).split(",");
        var cols_dx = cols_raw.filter(value => {return value.includes('DGNS') && !value.includes('VRSN')});
        var cols_dxtype = cols_raw.filter(value => {return value.includes('DGNS_VRSN')});
        
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                       WITH cte_unpvt as (
                        SELECT bene_id, clm_id, from_dt, thru_dt, rfr_npi,dx, dx_type,
                               TRIM(REGEXP_REPLACE(dx_idx, '[^[:digit:]]', ' ')) AS dgns_idx,
                               CASE WHEN dx_idx LIKE '%_E_%' THEN 'E' ELSE '' END AS dgns_mod,
                               CASE WHEN TRIM(REGEXP_REPLACE(dx_idx, '[^[:digit:]]', ' ')) = '1' THEN 'P' ELSE 'S' END AS pdx
                        FROM (SELECT * FROM ` + SRC_SCHEMA + `.`+ tbl_prefix +`_CLAIMS)
                        -- multi unpivot
                        UNPIVOT (dx for dx_idx in (` + cols_dx + `)) dx_unpvt
                        UNPIVOT (dx_type for dx_type_idx in (` + cols_dxtype + `)) dxtype_unpvt
                        WHERE RIGHT(dx_idx,3) = RIGHT(dx_type_idx,3) AND
                              TRIM(dx) is not null AND TRIM(dx) <> ''
                        )
                        SELECT `+ cols +`, '` + SRC_SCHEMA + `','`+ tbl_prefix +`_CLAIMS'
                        FROM cte_unpvt;`;
    } else {
        continue;
    }
    
      
    /** preview of the generated dynamic SQL scripts - comment it out when perform actual execution
       var log_stmt = snowflake.createStatement({
                        sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                        binds: [stg_pt_qry]});
       log_stmt.execute(); 
       // manual check `sp_output` entries and truncate afterwards
    **/
    
    // run dynamic dml query
    var add_new = snowflake.createStatement({sqlText: stg_pt_qry});
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    add_new.execute();
    commit_txn.execute();
}
$$
;
