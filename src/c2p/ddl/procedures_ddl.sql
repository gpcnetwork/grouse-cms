/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: procedures_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_PROCEDURES
# - PRIVATE_PROCEDURES_STAGE_XXX
*/
-- initialize table
create or replace table PRIVATE_PROCEDURES (
	 PROCEDURESID varchar(200)
	,PATID varchar(50) NOT NULL
	,ENCOUNTERID varchar(50) NOT NULL
	,ENC_TYPE varchar(2) NULL
	,ADMIT_DATE date NULL
	,PROVIDERID varchar(50) NULL
	,PX_DATE date NULL
	,PX varchar(11) NOT NULL
	,PX_TYPE varchar(2) NOT NULL
	,PX_SOURCE varchar(2) NULL
	,PPX varchar(2) NULL
	,RAW_PX varchar(50) NULL
	,RAW_PX_TYPE varchar(50) NULL
	,RAW_PPX varchar(50) NULL
    ,primary key (PROCEDURESID) 
);

-- initialize staging tables (multiple sources)
use schema CMS_PCORNET_CDM_STAGING;
create or replace table PRIVATE_PROCEDURES_STAGE_MEDPAR (
     BENE_ID varchar(20) NOT NULL
    ,MEDPARID varchar(20) NOT NULL
    ,TYPE_ADM varchar(3)
    ,SSLSSNF varchar(1)
    ,ADMSNDT date
    ,ORGNPINM varchar(20)
	,PX varchar(18)  
	,PX_TYPE varchar(2) 
    ,PX_IDX integer
    ,PX_DATE date
	,PPX varchar(2) 
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_PROCEDURES_STAGE_OUTPATIENT (
	 BENE_ID varchar(20)
	,CLM_ID varchar(20)
	,PROVIDER_NPI varchar(12)
	,PX varchar(18)  
	,PX_TYPE varchar(10) 
    ,PX_IDX integer
    ,PX_DATE date
	,PPX varchar(10) 
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_PROCEDURES_STAGE_HHA (
	 BENE_ID varchar(20)
	,CLM_ID varchar(20)
	,PROVIDER_NPI varchar(12)
	,PX varchar(18)  
	,PX_TYPE varchar(2) 
    ,PX_IDX integer
    ,PX_DATE date
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_PROCEDURES_STAGE_HOSPICE like PRIVATE_PROCEDURES_STAGE_HHA;

create or replace table PRIVATE_PROCEDURES_STAGE_BCARRIER (
	 BENE_ID varchar(20)
	,CLM_ID varchar(20)
	,PROVIDER_NPI varchar(12)
	,PX varchar(18)  
	,PX_TYPE varchar(2) 
    ,PX_IDX integer
    ,PX_DATE date
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_PROCEDURES_STAGE_DME (
	 BENE_ID varchar(20)
	,CLM_ID varchar(20)
	,PROVIDER_NPI varchar(12)
	,PX varchar(18)  
	,PX_TYPE varchar(2) 
    ,PX_IDX integer
    ,PX_DATE date
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);
