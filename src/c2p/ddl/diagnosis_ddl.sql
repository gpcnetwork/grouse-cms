/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: diagnosis_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DIAGNOSIS
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
