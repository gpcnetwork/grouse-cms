/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: encounter_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_ENCOUNTER
*/
-- initialize table
create or replace table ENCOUNTER (
     PATID varchar(50) NOT NULL 
    ,ENCOUNTERID varchar(50) NOT NULL 
    ,ENC_TYPE varchar(5) NOT NULL
    ,ADMIT_DATE date NOT NULL 
    -- ,ADMIT_TIME
    ,DISCHARGE_DATE date
    -- ,DISCHARGE_TIME
    ,PROVIDERID varchar(20)
    ,FACILITYID varchar(50)
    ,FACILITY_TYPE varchar(50)
    ,FACILITY_LOCATION varchar(5) 
    ,DISCHARGE_DISPOSITION varchar(5) 
    ,DISCHARGE_STATUS varchar(5)
    ,DRG varchar(5)
    ,DRG_TYPE varchar(5) --const:'02'
    ,ADMITTING_SOURCE varchar(5)
    ,PAYER_TYPE_PRIMARY varchar(5)
    ,PAYER_TYPE_SECONDARY varchar(5)
    -- ,RAW_SITEID varchar(50)
    ,RAW_ENC_TYPE varchar(50)
    ,RAW_DISCHARGE_DISPOSITION varchar(50) 
    ,RAW_DISCHARGE_STATUS varchar(50)
    ,RAW_DRG_TYPE varchar(50)
    ,RAW_ADMITTING_SOURCE varchar(50)
    ,RAW_FACILITY_TYPE varchar(50)
    ,RAW_FACILITY_CODE varchar(50)
    ,RAW_PAYER_TYPE_PRIMARY varchar(50)
    -- ,RAW_PAYER_NAME_PRIMARY varchar(50)
    -- ,RAW_PAYER_ID_PRIMARY varchar(50)
    -- ,RAW_PAYER_TYPE_SECONDARY varchar(50)
    -- ,RAW_PAYER_NAME_SECONDARY varchar(50) 
    -- ,RAW_PAYER_ID_SECONDARY varchar(50)
    ,primary key (PATID, ENCOUNTERID)
);
