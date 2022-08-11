/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: provider_stg.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for staging data from 
#               MEDPAR, INSTITUTIONAL, NON-INSTITUTIONAL, and PDE files in preparationg for 
#               CDM PROVIDER table transformation
*/

create or replace procedure stage_provider(SRC_SCHEMA STRING)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SRC_SCHEMA: source schema for staging
**/
// collect columns from target staging table
var get_stg_cols = snowflake.createStatement({
    sqlText: `SELECT table_schema,LISTAGG(DISTINCT column_name,',') AS enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = 'CMS_PCORNET_CDM_STAGING'
                  AND table_name = 'PRIVATE_PROVIDER_STAGE'
                  AND column_name NOT IN ('NPI','SRC_SCHEMA','SRC_TABLE')
                GROUP BY table_schema;`});
var stg_cols = get_stg_cols.execute(); stg_cols.next();
var tbl_schema = stg_cols.getColumnValue(1);
var stg_cols_lst = stg_cols.getColumnValue(2).split(",");
var stg_cols_alias = stg_cols_lst.map(value => {return 'nppes.'+ value});
var stg_cols_alias2 = stg_cols_lst.map(value => {return 's.' + value});
var path_to_nppes = tbl_schema +`.NPIDATA`

// collect NPI columns from source tables
var get_npi_cols = snowflake.createStatement({
    sqlText: `SELECT table_name, LISTAGG(DISTINCT column_name,',') AS enc_col
                FROM information_schema.columns 
                WHERE table_catalog = current_database() 
                  AND table_schema = :1
                  AND (column_name LIKE '%_NPI%' OR column_name LIKE '%ORGNPI%')
                GROUP BY table_name;`,
    binds: [SRC_SCHEMA]});
var npi_cols = get_npi_cols.execute();

// for each staging table
while (npi_cols.next())
{
    // parameters for target table
    var src_tbl = npi_cols.getColumnValue(1);
    var src_cols = npi_cols.getColumnValue(2).split(",");
    var stg_pt_qry = `MERGE INTO PRIVATE_PROVIDER_STAGE t
                        USING (
                            WITH cte_unpvt as (
                                SELECT npi, npi_col 
                                FROM `+ SRC_SCHEMA +`.`+ src_tbl +`
                                UNPIVOT (npi for npi_col in (` + src_cols + `))
                                WHERE npi IS NOT NULL AND npi <> ''
                               )
                               SELECT DISTINCT
                                      cte_unpvt.npi AS npi,
                                      `+ stg_cols_alias +`,
                                      '`+ SRC_SCHEMA +`' AS src_schema,
                                      '`+ src_tbl +`' AS src_table
                               FROM cte_unpvt 
                               JOIN `+ path_to_nppes +` nppes 
                               ON cte_unpvt.NPI = to_char(nppes.NPI)
                         ) s
                         ON t.npi = s.npi AND t.src_schema = s.src_schema
                         WHEN NOT MATCHED
                            THEN INSERT(npi,`+ stg_cols_lst +`,src_schema,src_table) 
                                VALUES(s.npi,`+ stg_cols_alias2 +`,s.src_schema,s.src_table);`;      
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



