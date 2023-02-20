/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: procedures_stg.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for staging data from 
#               MEDPAR, INSTITUTIONAL, and NON-INSTITUTIONAL trailer files in preparationg for 
#               CDM PROCEDURES table transformation
*/
create or replace procedure stage_procedures(SRC_SCHEMA STRING, PART STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
 * @param {string} PART: part name of source table, NULL suggests all mappable tables
**/

// collect columns from target staging table
var get_tbl_cols = snowflake.createStatement({
    sqlText: `SELECT table_name, listagg(column_name,',') AS enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = 'CMS_PCORNET_CDM_STAGING'
                  AND table_name LIKE 'PRIVATE_PROCEDURES_STAGE%'
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
       
    if(table.includes('MEDPAR') && (table.includes(PART)||(PART===undefined))){
        // parameters for source table
        var collate_col_stmt = `SELECT table_name, listagg(column_name,',') as cols 
                                FROM information_schema.columns 
                                WHERE table_catalog = 'GROUSE_DB' 
                                    AND table_name like 'MEDPAR%' 
                                    AND table_schema = '`+ SRC_SCHEMA +`' 
                                    AND column_name like '%PRCDR%' AND column_name NOT like '%CNT'AND column_name NOT like '%SW'
                                GROUP BY table_name;`
        var get_cols_raw_stmt = snowflake.createStatement({sqlText: collate_col_stmt});
        var get_cols_raw = get_cols_raw_stmt.execute(); get_cols_raw.next();
        var cols_raw = get_cols_raw.getColumnValue(2).split(",");
        var cols_px = cols_raw.filter(value => {return value.includes('PRCDRCD')});
        var cols_pxtype = cols_raw.filter(value => {return value.includes('PRCDR_VRSN')});
        var cols_pxdt = cols_raw.filter(value => {return value.includes('PRCDRDT')});
    
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                       WITH cte_unpvt as (
                         SELECT bene_id, medparid, type_adm, sslssnf, admsndt, orgnpinm,
                                TRIM(REGEXP_SUBSTR(px_idx,'[0-9]+')) px_idx,
                                CASE WHEN pxv = '9' THEN '09' WHEN pxv = '0' THEN '10' ELSE pxv END AS px_type,
                                px AS px, pxdt as px_date,
                                CASE WHEN TRIM(REGEXP_SUBSTR(px_idx,'[0-9]+')) = '1' THEN 'P' ELSE 'S' END AS ppx
                        FROM (SELECT * FROM ` + SRC_SCHEMA + `.MEDPAR_ALL)
                        UNPIVOT (px for px_idx in (` + cols_px + `)) x
                        UNPIVOT (pxv for pxv_idx in (` + cols_pxtype + `)) y
                        UNPIVOT (pxdt for pxdt_idx in (` + cols_pxdt + `)) z
                        WHERE REGEXP_SUBSTR(px_idx,'[0-9]+') = REGEXP_SUBSTR(pxv_idx,'[0-9]+') AND
                              REGEXP_SUBSTR(px_idx,'[0-9]+') = REGEXP_SUBSTR(pxdt_idx,'[0-9]+') AND
                              TRIM(px) is not null and TRIM(px) <> '' 
                       )
                       SELECT `+ cols +`, '`+ SRC_SCHEMA +`', 'MEDPAR_ALL'
                       FROM cte_unpvt;`;
                       
    } else if(table.includes('OUTPATIENT') && (table.includes(PART)||(PART===undefined))){
        // parameters for source table
        var collate_col_stmt = `SELECT table_name, listagg(column_name,',') as cols 
                                FROM information_schema.columns 
                                WHERE table_catalog = 'GROUSE_DB' 
                                    AND table_name = 'OUTPATIENT_BASE_CLAIMS' 
                                    AND table_schema = '`+ SRC_SCHEMA +`' 
                                    AND column_name like '%PRCDR%'
                                GROUP BY table_name;`
        var get_cols_raw_stmt = snowflake.createStatement({sqlText: collate_col_stmt});
        var get_cols_raw = get_cols_raw_stmt.execute(); get_cols_raw.next();
        var table_raw = get_cols_raw.getColumnValue(1); 
        var cols_raw = get_cols_raw.getColumnValue(2).split(",");
        var table_alt = table_raw.replace('BASE_CLAIMS','REVENUE_CENTER');
        var cols_px = cols_raw.filter(value => {return value.includes('PRCDR_CD')});
        var cols_pxtype = cols_raw.filter(value => {return value.includes('PRCDR_VRSN')});
        var cols_pxdt = cols_raw.filter(value => {return value.includes('PRCDR_DT')});
        
        // no icd version columns in medicare data after version K (>=2014)
        var pxtype_phrase = [];
        if (cols_pxtype === undefined || cols_pxtype.length == 0) {
            pxtype_phrase.push('null AS px_type');
            pxtype_phrase.push('');
            pxtype_phrase.push(`AND TRIM(px) is not null AND TRIM(px) <> ''`);
        } else {
            pxtype_phrase.push('px_type');
            pxtype_phrase.push(`UNPIVOT (px_type for pxtype_idx in (` + cols_pxtype + `)) pxtype_unpvt`);
            pxtype_phrase.push(`AND REGEXP_SUBSTR(px_idx,'[[:digit:]]') = REGEXP_SUBSTR(pxtype_idx,'[[:digit:]]') AND TRIM(px) is not null and TRIM(px) <> ''`);
        }
    
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                       WITH cte_unpvt as (
                        SELECT bene_id, clm_id, from_dt, op_npi, at_npi, px, `+ pxtype_phrase[0] +`, px_date,
                                TRIM(REGEXP_SUBSTR(px_idx,'[0-9]+')) px_idx,
                                CASE WHEN TRIM(REGEXP_SUBSTR(px_idx,'[0-9]+')) = '1' THEN 'P' ELSE 'S' END AS ppx
                        FROM (SELECT * FROM ` + SRC_SCHEMA + `.`+ table_raw +`)
                        -- multi unpivot
                        UNPIVOT (px for px_idx in (` + cols_px + `)) px_unpvt 
                        UNPIVOT (px_date for pxdt_idx in (` + cols_pxdt + `)) pxdt_unpvt
                        `+ pxtype_phrase[1] +`
                        WHERE REGEXP_SUBSTR(px_idx,'[[:digit:]]') = REGEXP_SUBSTR(pxdt_idx,'[[:digit:]]')
                              `+ pxtype_phrase[2] +`
                        ), multi_part AS (
                        SELECT bene_id,clm_id,NVL(NULLIF(TRIM(op_npi),''),NULLIF(TRIM(at_npi),'')) AS provider_npi,px,px_type,px_date,ppx,'`+ table_raw +`' AS src_table,px_idx
                        FROM cte_unpvt
                        UNION
                        SELECT a.bene_id,a.clm_id,NVL(NULLIF(TRIM(a.rndrng_physn_npi),''),NULLIF(TRIM(b.at_npi),'')),a.hcpcs_cd,'CH',NVL(a.rev_dt,a.thru_dt),'NI','`+ table_alt +`',
                               ROW_NUMBER() OVER (PARTITION BY a.bene_id,a.clm_id ORDER BY NVL(a.rev_dt,a.thru_dt))
                        FROM `+ SRC_SCHEMA +`.`+ table_alt +` a
                        JOIN `+ SRC_SCHEMA +`.`+ table_raw +` b ON a.bene_id = b.bene_id AND a.clm_id = b.clm_id
                        WHERE a.hcpcs_cd <> ''   
                        UNION
                        SELECT a.bene_id,a.clm_id,NVL(NULLIF(TRIM(a.rndrng_physn_npi),''),NULLIF(TRIM(b.at_npi),'')),a.rev_cntr,'RE',NVL(a.rev_dt,a.thru_dt),'NI','`+ table_alt +`',
                               ROW_NUMBER() OVER (PARTITION BY a.bene_id,a.clm_id ORDER BY NVL(a.rev_dt,a.thru_dt))
                        FROM `+ SRC_SCHEMA +`.`+ table_alt +` a
                        JOIN `+ SRC_SCHEMA +`.`+ table_raw +` b ON a.bene_id = b.bene_id AND a.clm_id = b.clm_id
                        WHERE a.hcpcs_cd = ''
                        )
                        SELECT `+ cols +`, '`+ SRC_SCHEMA +`', src_table
                        FROM multi_part;`;
                     
    } else if((table.includes('HHA') || table.includes('HOSPICE')) && (table.includes(PART)||(PART===undefined))){
        // parameters for source table
        var table_raw = table.includes('HHA') ? 'HHA_BASE_CLAIMS' : 'HOSPICE_BASE_CLAIMS';
        var table_alt = table_raw.replace('BASE_CLAIMS','REVENUE_CENTER');
        
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                        WITH multi_part AS (
                            SELECT a.bene_id,a.clm_id,NVL(NULLIF(TRIM(a.rndrng_physn_npi),''),NULLIF(TRIM(b.at_npi),'')) AS provider_npi,a.hcpcs_cd AS px,
                                   'CH' AS px_type,NVL(a.rev_dt,a.thru_dt) AS px_date,
                                   ROW_NUMBER() OVER (PARTITION BY a.bene_id,a.clm_id ORDER BY NVL(a.rev_dt,a.thru_dt)) AS px_idx
                            FROM `+ SRC_SCHEMA +`.`+ table_alt +` a
                            JOIN `+ SRC_SCHEMA +`.`+ table_raw +` b ON a.bene_id = b.bene_id AND a.clm_id = b.clm_id 
                            WHERE a.hcpcs_cd <> '' AND a.hcpcs_cd is not null
                            UNION
                            SELECT a.bene_id,a.clm_id,NVL(NULLIF(TRIM(a.rndrng_physn_npi),''),NULLIF(TRIM(b.at_npi),'')),a.rev_cntr,'RE',NVL(a.rev_dt,a.thru_dt),
                                   ROW_NUMBER() OVER (PARTITION BY a.bene_id,a.clm_id ORDER BY NVL(a.rev_dt,a.thru_dt))
                            FROM `+ SRC_SCHEMA +`.`+ table_alt +` a
                            JOIN `+ SRC_SCHEMA +`.`+ table_raw +` b ON a.bene_id = b.bene_id AND a.clm_id = b.clm_id 
                            WHERE a.hcpcs_cd = '' OR a.hcpcs_cd is null
                        )
                        SELECT `+ cols +`, '`+ SRC_SCHEMA +`', '`+ table_raw +`'
                        FROM multi_part;`;
    
    
    } else if((table.includes('BCARRIER') || table.includes('DME')) && (table.includes(PART)||(PART===undefined))){
        // parameters for source table
        var table_raw = table.includes('BCARRIER') ? 'BCARRIER_LINE' : 'DME_LINE';
        var collate_col_stmt = `SELECT table_name, listagg(column_name,',') as cols  
                                FROM information_schema.columns 
                                WHERE table_catalog = 'GROUSE_DB' 
                                    AND table_name = '`+ table_raw +`'
                                    AND table_schema = '`+ SRC_SCHEMA +`' 
                                    AND column_name in ('PRF_NPI','SUP_NPI')
                                GROUP BY table_name;`
        var get_cols_raw_stmt = snowflake.createStatement({sqlText: collate_col_stmt});
        var get_cols_raw = get_cols_raw_stmt.execute(); get_cols_raw.next();
        var table_raw = get_cols_raw.getColumnValue(1);
        var npi_col = get_cols_raw.getColumnValue(2);
        
        stg_pt_qry += `INSERT INTO `+ table +`(`+ cols +`,src_schema,src_table)
                        WITH cte_mapping AS (
                         SELECT bene_id, clm_id,`+ npi_col +` AS provider_npi,hcpcs_cd AS px,'CH' AS px_type, expnsdt1 AS px_date,
                                ROW_NUMBER() OVER (PARTITION BY bene_id,clm_id ORDER BY expnsdt1) AS px_idx
                         FROM ` + SRC_SCHEMA + `.`+ table_raw +`
                        )
                        SELECT `+ cols +`, '`+ SRC_SCHEMA +`', '`+ table_raw +`'
                        FROM cte_mapping;`;
    } else {
        continue;
    }
    
    /*
    // preview of the generated dynamic SQL scripts - comment it out when perform actual execution
       var log_stmt = snowflake.createStatement({
                        sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                        binds: [stg_pt_qry]});
       log_stmt.execute(); 
    */
    
    // run dynamic dml query
    var add_new = snowflake.createStatement({sqlText: stg_pt_qry});
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    add_new.execute();
    commit_txn.execute();
}
$$
;



