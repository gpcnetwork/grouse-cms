/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: death_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               MBSF denominator files into CDM DEATH table 
*/

create or replace procedure transform_to_death(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/

// full-load or cdc-based load
let subset_clause = '';
if (SRC_SCHEMA !== undefined) {
    // identify source table name (considering potential variations)
    var collect_src_stmt = snowflake.createStatement({
        sqlText: `SELECT table_schema, table_name
                    FROM information_schema.tables 
                    WHERE table_catalog = 'GROUSE_DB' 
                      AND table_name like 'MBSF_AB%'
                      AND table_schema = :1;`,
         binds: [SRC_SCHEMA]});
    var get_table = collect_src_stmt.execute(); get_table.next();
    var src_tbl = get_table.getColumnValue(2);
    subset_clause += (SRC_SCHEMA === undefined) ? '': `WHERE src_schema = '` + SRC_SCHEMA + `' AND src_table = '` + src_tbl + `'`;
}

// generate dynamic dml query
var t_qry = `MERGE INTO PRIVATE_DEATH t
             USING (
                WITH tr_cte AS (
                    SELECT  bene_id AS patid
                           ,death_dt AS death_date
                           ,CASE WHEN v_dod_sw='V' THEN 'N' ELSE 'D' END AS death_date_impute
                           ,CASE WHEN v_dod_sw='V' THEN 'S' ELSE 'L' END AS death_source
                           ,row_number() over (partition by bene_id order by src_date desc) AS rn
                    FROM CMS_PCORNET_CDM_STAGING.private_death_stage `+ subset_clause +` 
                )
                SELECT * FROM tr_cte WHERE rn = 1
             ) s 
             ON t.patid = s.patid
             WHEN MATCHED AND s.death_date >= t.death_date 
                 THEN UPDATE SET t.death_date = s.death_date, t.death_date_impute = s.death_date_impute, t.death_source = s.death_source
             WHEN NOT MATCHED 
                 THEN INSERT (patid,death_date,death_date_impute,death_source) 
                     VALUES (s.patid,s.death_date,s.death_date_impute,s.death_source);`;

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry});
run_transform_dml.execute();
commit_txn.execute();
$$
;

