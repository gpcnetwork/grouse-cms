/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: diagnosis_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DIAGNOSIS_STAGE_MEDPAR
# - PRIVATE_DIAGNOSIS_STAGE_OUTPATIENT
# - PRIVATE_DIAGNOSIS_STAGE_HHA
# - PRIVATE_DIAGNOSIS_STAGE_HOSPICE
# - PRIVATE_DIAGNOSIS_STAGE_BCARRIER
# - PRIVATE_DIAGNOSIS_STAGE_DME
*/

-- initialize staging tables (multiple sources)
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

