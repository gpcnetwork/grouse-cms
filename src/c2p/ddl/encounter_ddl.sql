/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: encounter_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_ENCOUNTER
*/
-- initialize table
create or replace table PRIVATE_ENCOUNTER (
     PATID varchar(50) NOT NULL -- bene_id
    ,ENCOUNTERID varchar(50) NOT NULL -- medparid; clm_id
    ,ENC_TYPE varchar(5) NOT NULL -- SSLSSNF,ER_AMT; 
    ,ADMIT_DATE date NOT NULL -- ADMSNDT
--    ,ADMIT_TIME
    ,DISCHARGE_DATE date -- DSCHRGDT, CVRLVLDT
--    ,DISCHARGE_TIME
    ,PROVIDERID varchar(20) -- ORGNPINM; AT_NPI
    ,FACILITYID varchar(50) -- PRVDRNUM/PROVIDER 
    ,FACILITY_TYPE varchar(50) -- g(f(PRVDRNUM/PROVIDER))
--    ,FACILITY_LOCATION varchar(5) -- f(PRVDRNUM/PROVIDER)
    ,DISCHARGE_DISPOSITION varchar(5) -- f(DSCHRGCD)
    ,DISCHARGE_STATUS varchar(5) -- f(DSTNTNCD)
    ,DRG varchar(5) -- DRG_CD
    ,DRG_TYPE varchar(5) --const:'02'
    ,ADMITTING_SOURCE varchar(5) -- f(SRC_ADMS)
    ,PAYER_TYPE_PRIMARY varchar(5) -- f(PRPAY_CD)
    ,PAYER_TYPE_SECONDARY varchar(5) -- f(PRPAY_CD)
--    ,RAW_SITEID varchar(50)
    ,RAW_ENC_TYPE varchar(50)
    ,RAW_DISCHARGE_DISPOSITION varchar(50) -- DSCHRGCD
    ,RAW_DISCHARGE_STATUS varchar(50) -- DSTNTNCD
    ,RAW_DRG_TYPE varchar(50)
    ,RAW_ADMITTING_SOURCE varchar(50) -- DSTNTNCD
    ,RAW_FACILITY_TYPE varchar(50) -- f(PRVDRNUM)
    ,RAW_FACILITY_CODE varchar(50)
    ,RAW_PAYER_TYPE_PRIMARY varchar(50) -- PRPAY_CD
--    ,RAW_PAYER_NAME_PRIMARY varchar(50)
--    ,RAW_PAYER_ID_PRIMARY varchar(50)
--    ,RAW_PAYER_TYPE_SECONDARY varchar(50) -- PRPAY_CD
--    ,RAW_PAYER_NAME_SECONDARY varchar(50) 
--    ,RAW_PAYER_ID_SECONDARY varchar(50)
    ,primary key (PATID, ENCOUNTERID)
);
