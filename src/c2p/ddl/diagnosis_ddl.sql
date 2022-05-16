/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: diagnosis_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DIAGNOSIS
# - PRIVATE_DIAGNOSIS_STAGE_XXX
*/
-- initialize table
create or replace table PRIVATE_DIAGNOSIS (
	 DIAGNOSISID varchar(50) NOT NULL 
	,PATID varchar(20) NOT NULL
	,ENCOUNTERID varchar(20) NOT NULL
	,ENC_TYPE varchar(2) NULL 
	,ADMIT_DATE date NULL 
	,DX_DATE date NULL 
	,PROVIDERID varchar(20) NULL
	,DX varchar(18) NULL 
	,DX_TYPE varchar(2) NULL
	,DX_SOURCE varchar(2) NULL 
    ,DX_ORIGIN varchar(2) NULL
	,PDX varchar(2) NULL
	,DX_POA varchar(2) NULL
	,RAW_DX varchar(50) NULL
	,RAW_DX_TYPE varchar(50) NULL
	,RAW_DX_SOURCE varchar(50) NULL
	,RAW_PDX varchar(50) NULL
	,RAW_DX_POA varchar(50) NULL
    ,primary key (DIAGNOSISID) 
);

-- initialize staging tables (multiple sources)
use schema CMS_PCORNET_CDM_STAGING;
create or replace table PRIVATE_DIAGNOSIS_STAGE_MEDPAR (
     BENE_ID varchar(20) NOT NULL
    ,MEDPARID varchar(20) NOT NULL
    ,TYPE_ADM varchar(3)
    ,SSLSSNF varchar(1)
    ,ADMSNDT date
    ,CVRLVLDT date
	,QLFYTHRU date
    ,DSCHRGDT date
    ,ORGNPINM varchar(20)
	,DX varchar(18)  
	,DX_TYPE varchar(2) 
    ,DX_POA varchar(2)
    ,DX_IDX varchar(2)
    ,DX_MOD varchar(2)
	,DX_SOURCE varchar(2)  
	,PDX varchar(2) 
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_DIAGNOSIS_STAGE_OUTPATIENT (
	 BENE_ID varchar(20)
	,CLM_ID varchar(20)
	,FROM_DT date
	,THRU_DT date
	,AT_NPI varchar(12)
	,DX varchar(18)  
	,DX_TYPE varchar(2) 
    ,DX_POA varchar(2)
    ,DX_IDX varchar(2)
    ,DX_MOD varchar(2)
	,PDX varchar(2) 
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_DIAGNOSIS_STAGE_HHA like PRIVATE_DIAGNOSIS_STAGE_OUTPATIENT;

create or replace table PRIVATE_DIAGNOSIS_STAGE_HOSPICE like PRIVATE_DIAGNOSIS_STAGE_OUTPATIENT;

create or replace table PRIVATE_DIAGNOSIS_STAGE_BCARRIER (
	 BENE_ID varchar(20)
	,CLM_ID varchar(20)
	,FROM_DT date
	,THRU_DT date
	,RFR_NPI varchar(12)
	,DX varchar(18)  
	,DX_TYPE varchar(2) 
    ,DX_POA varchar(2)
    ,DX_IDX varchar(2)
    ,DX_MOD varchar(2)
	,PDX varchar(2) 
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);

create or replace table PRIVATE_DIAGNOSIS_STAGE_DME like PRIVATE_DIAGNOSIS_STAGE_BCARRIER;

