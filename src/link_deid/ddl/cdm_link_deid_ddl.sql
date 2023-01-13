/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid_ddl.sql                                                 
# Description: Create table shells for multi-id mapping
# Note: May need to run this under a non-mapping schema
*/

/*create master BENE_XWALK, can cumulate over time*/
create or replace table BENE_MAPPING.BENE_XWALK_CMS (
    BENE_ID VARCHAR(100) NOT NULL,
    BENE_ID_HASH VARCHAR(100),
    BENE_DOB DATE,
    BENE_DOB_DEID DATE,
    HASHID VARCHAR(100),
    SITEID VARCHAR(20),
    SHIFT NUMBER(4,0),
    SEED NUMBER(3,0) -- for reproducing random date shifts
);

create or replace table GEOID_MAPPING.ADDRESSID_XWALK_CMS (
    ADDRESSID VARCHAR(100) NOT NULL,
    ADDRESSID_HASH VARCHAR(100) NOT NULL
);

create or replace table GEOID_MAPPING.GEOCODEID_XWALK_CMS (
    GEOCODEID VARCHAR(100) NOT NULL,
    GEOCODEID_HASH VARCHAR(100) NOT NULL
);

/*create site PATID_XWALK_<SITE>*/
create or replace procedure patid_xwalk_ddl(SITES array)
returns variant
language javascript
as
$$
/**
 * Stored procedure to create table shells for patid_xwalk_<site>
 * @param {array} SITES: an array of site acronyms (matching schema name suffix)
*/

var i;
for(i=0; i<SITES.length; i++){
    // contruct query
    var site = SITES[i].toString();   
    var ddl_qry = `CREATE OR REPLACE TABLE PATID_MAPPING.PATID_XWALK_`+ site +` (
                        PATID VARCHAR(100) NOT NULL,
                        PATID_HASH VARCHAR(100),
                        PAT_DOB DATE,
                        PAT_DOB_DEID DATE,
                        SHIFT NUMBER(4,0),
                        SEED NUMBER(3,0)
                       );`    
    // execution
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    var run_ddl_qry = snowflake.createStatement({sqlText: ddl_qry});
    run_ddl_qry.execute();
    commit_txn.execute();
}
$$
;

call patid_xwalk_ddl(array_construct(
     'ALLINA'
    ,'IHC'
    ,'KUMC'
    ,'MCRI'
    ,'MCW'
    ,'MU'
    ,'UIOWA'
    ,'UNMC'
    ,'UTHOUSTON'
    ,'UTHSCSA'
    ,'UTSW'
    ,'UU'
    ,'WASHU'
));
