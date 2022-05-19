/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_LDS_ADDRESS_HISTORY_STAGE
*/

-- initialize staging table
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
