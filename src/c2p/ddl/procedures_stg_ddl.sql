/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: procedures_stg_ddl.sql                                                 
# - PRIVATE_PROCEDURES_STAGE_MEDPAR
# - PRIVATE_PROCEDURES_STAGE_OUTPATIENT
# - PRIVATE_PROCEDURES_STAGE_HHA
# - PRIVATE_PROCEDURES_STAGE_HOSPICE
# - PRIVATE_PROCEDURES_STAGE_BCARRIER
# - PRIVATE_PROCEDURES_STAGE_DME
*/

-- initialize staging tables (multiple sources)
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
