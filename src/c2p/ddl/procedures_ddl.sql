/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: procedures_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_PROCEDURES
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
