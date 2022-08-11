/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: provider_c2p.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for transforming data from 
#               MEDPAR, INSTITUTIONAL, NON-INSTITUTIONAL, and PDE files in CDM PROVIDER table
*/

create or replace procedure transform_to_provider(SRC_SCHEMA STRING)
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
    subset_clause +=  `WHERE src_schema = '` + SRC_SCHEMA + `'`;
}

// generate dynamtic query
var t_qry = `MERGE INTO private_provider t
             USING (
                SELECT npi AS providerid,
                       provider_gender_code AS provider_sex,
                       healthcare_provider_taxonomy_code_1 AS provider_specialty_primary,
                       npi AS provider_npi,
                       'Y' AS provider_npi_flag,
                       healthcare_provider_taxonomy_code_1 AS raw_provider_specialty_primary
                FROM cms_pcornet_cdm_staging.private_provider_stage
                `+ subset_clause + `
             ) s
             ON t.provider_npi = s.providerid
             WHEN NOT MATCHED
                THEN INSERT (providerid, provider_sex, provider_specialty_primary, provider_npi, provider_npi_flag, raw_provider_specialty_primary)
                    VALUES (s.providerid, s.provider_sex, s.provider_specialty_primary, s.provider_npi, s.provider_npi_flag, s.raw_provider_specialty_primary);`;

/**
// preview of the generated dynamic SQL scripts - comment it out when perform actual execution
   var log_stmt = snowflake.createStatement({
                    sqlText: `INSERT INTO dev.sp_output (qry) values (:1);`,
                    binds: [stg_pt_qry]});
   log_stmt.execute(); 
**/

// run dynamic dml query
var commit_txn = snowflake.createStatement({sqlText: `commit;`});
var run_transform_dml = snowflake.createStatement({sqlText: t_qry});
run_transform_dml.execute();
commit_txn.execute();  
$$
;