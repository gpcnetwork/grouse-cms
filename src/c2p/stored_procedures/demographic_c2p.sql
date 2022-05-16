/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: demographic_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming 
#               MBSF denominator files into CDM DEMOINATOR table 
*/

create or replace procedure transform_to_demographic(SRC_SCHEMA STRING)
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
    subset_clause +=  `WHERE src_schema = '` + SRC_SCHEMA + `' AND src_table = '` + src_tbl + `'`;
}

// generate dynamtic query
var t_qry = `MERGE INTO private_demographic t
             USING (
                WITH tr_cte AS (
                    SELECT bene_id as patid
                          ,bene_dob as birth_date
                          ,CASE WHEN sex = '2' THEN 'F'
                                WHEN sex = '1' THEN 'M'
                                WHEN sex = '0' THEN 'UN'
                                ELSE 'NI'
                           END AS sex
                          ,CASE WHEN rti_race_cd = '5' THEN 'Y'
                                ELSE 'NI'
                           END AS hispanic
                          ,CASE WHEN rti_race_cd = '1' THEN '05'
                                WHEN rti_race_cd = '2' THEN '03'
                                WHEN rti_race_cd = '3' THEN 'OT'
                                WHEN rti_race_cd = '4' THEN '02'
                                WHEN rti_race_cd = '6' THEN '01'
                                ELSE 'NI'
                           END AS race                 
                          ,sex AS raw_sex
                          ,rti_race_cd AS raw_hispanic
                          ,rti_race_cd AS raw_race
                          ,row_number() over (partition by bene_id order by src_date desc) AS rn
                    FROM CMS_PCORNET_CDM_STAGING.private_demographic_stage `+ subset_clause +`
                )
                SELECT * FROM tr_cte WHERE rn = 1
             ) s 
             ON t.patid = s.patid
                WHEN MATCHED AND s.birth_date is not NULL THEN UPDATE SET t.birth_date = s.birth_date
                WHEN MATCHED AND s.sex in ('F','M') THEN UPDATE SET t.sex = s.sex
                WHEN MATCHED AND s.race not in ('OT','NI',NULL) THEN UPDATE SET t.race = s.race
                WHEN MATCHED AND s.hispanic in ('Y','N') THEN UPDATE SET t.hispanic = s.hispanic                    
                WHEN MATCHED AND s.sex in ('F','M') THEN UPDATE SET t.raw_sex = s.raw_sex
                WHEN MATCHED AND s.race not in ('OT','NI',NULL) THEN UPDATE SET t.raw_race = s.raw_race
                WHEN MATCHED AND s.hispanic in ('Y','N') THEN UPDATE SET t.raw_hispanic = s.raw_hispanic                    
                WHEN NOT MATCHED 
                    THEN INSERT (patid,birth_date,sex,hispanic,race,raw_sex,raw_hispanic,raw_race) 
                        VALUES (s.patid,s.birth_date,s.sex,s.hispanic,s.race,s.raw_sex,s.raw_hispanic,s.raw_race);`;

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry});
run_transform_dml.execute();
commit_txn.execute();  
$$
;

