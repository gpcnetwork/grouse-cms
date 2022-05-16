/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_LDS_ADDRESS_HISTORY
# - PRIVATE_LDS_ADDRESS_HISTORY_STAGE
*/
-- initialize final table
create or replace table PRIVATE_LDS_ADDRESS_HISTORY ( 
    ADDRESSID VARCHAR(20 BYTE) NOT NULL, 
	PATID VARCHAR(50 BYTE) NOT NULL, 
	ADDRESS_USE VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_TYPE VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_PREFERRED VARCHAR(2 BYTE) NOT NULL, 
    ADDRESS_CITY VARCHAR(50 BYTE), 
    ADDRESS_COUNTY VARCHAR(50 BYTE), 
	ADDRESS_STATE VARCHAR(2 BYTE), 
	ADDRESS_ZIP5 VARCHAR(5 BYTE),  
	ADDRESS_ZIP9 VARCHAR(9 BYTE),
	ADDRESS_PERIOD_START DATE NOT NULL,
    ADDRESS_PERIOD_END DATE
);

-- initialize staging table
use schema CMS_PCORNET_CDM_STAGING; 
create or replace table PRIVATE_LDS_ADDRESS_HISTORY_STAGE ( 
     BENE_ID varchar(50) NOT NULL
    ,RFRNC_YR varchar(5) NOT NULL
    ,CNTY_CD varchar(5) 
    ,STATE_CD varchar(5)
    ,ZIP_CD varchar(10)
    ,SRC_SCHEMA varchar(20) NOT NULL
    ,SRC_TABLE varchar(30) NOT NULL
    ,SRC_DATE date NOT NULL
);
