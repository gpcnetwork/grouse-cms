/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: provider_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_PROVIDER
# - PRIVATE_PROVIDER_STAGE_XXX
# https://marcusrauhut.com/python-for-marketers-pulling-data-from-the-npi-registry/
*/
-- initialize table
create or replace table PRIVATE_PROVIDER (
	SOURCE_PROVIDER_ID VARCHAR(20),
	SOURCE_PROVIDER_ID_NAME VARCHAR(50),
	PROVIDERID VARCHAR(50),
	PROVIDER_SEX VARCHAR(2),
	PROVIDER_SPECIALTY_PRIMARY VARCHAR(50),
	PROVIDER_NPI NUMBER(18,0),
	PROVIDER_NPI_FLAG VARCHAR(1),
	RAW_PROVIDER_SPECIALTY_PRIMARY VARCHAR(50)
);

-- initialize staging tables (multiple sources)
use schema CMS_PCORNET_CDM_STAGING;
create or replace table PRIVATE_PROVIDER_STAGE_MEDPAR (
     NPI NUMBER(18,0)
	,PROVIDER_LAST_NAME_LEGAL_NAME VARCHAR(40)
	,PROVIDER_FIRST_NAME VARCHAR(40)
	,PROVIDER_MIDDLE_NAME VARCHAR(20)
	,PROVIDER_NAME_PREFIX_TEXT VARCHAR(10)
	,PROVIDER_NAME_SUFFIX_TEXT VARCHAR(10)
	,PROVIDER_CREDENTIAL_TEXT VARCHAR(50)
	,PROVIDER_GENDER_CODE VARCHAR(20)
	,LAST_UPDATE_DATE VARCHAR(20)
	,NPI_DEACTIVATION_REASON_CODE VARCHAR(20)
	,NPI_DEACTIVATION_DATE VARCHAR(20)
	,NPI_REACTIVATION_DATE VARCHAR(20)
	,HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 VARCHAR(20)
	,PROVIDER_LICENSE_NUMBER_1 VARCHAR(20)
	,PROVIDER_LICENSE_NUMBER_STATE_CODE_1 VARCHAR(20)
	,HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_1 VARCHAR(20)
	,HEALTHCARE_PROVIDER_TAXONOMY_CODE_2 VARCHAR(20)
	,PROVIDER_LICENSE_NUMBER_2 VARCHAR(20)
	,PROVIDER_LICENSE_NUMBER_STATE_CODE_2 VARCHAR(20)
	,HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_2 VARCHAR(20)
	,HEALTHCARE_PROVIDER_TAXONOMY_CODE_3 VARCHAR(20)
	,PROVIDER_LICENSE_NUMBER_3 VARCHAR(20)
	,PROVIDER_LICENSE_NUMBER_STATE_CODE_3 VARCHAR(20)
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);