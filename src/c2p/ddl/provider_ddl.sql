/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: provider_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_PROVIDER
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
