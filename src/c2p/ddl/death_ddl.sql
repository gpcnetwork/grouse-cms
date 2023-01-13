/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: death_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DEATH
*/
-- initialize final table
create or replace table PRIVATE_DEATH (
	 PATID varchar(50) NOT NULL --BENE_ID
	,DEATH_DATE date NULL --DEATH_DT
	,DEATH_DATE_IMPUTE varchar(2) NULL --if V_DOD_SW='V':'N', 'D'
	,DEATH_SOURCE varchar(2) NULL ---if V_DOD_SW='V':'S', 'L'
	-- ,DEATH_MATCH_CONFIDENCE varchar(2) -- NULL
    ,primary key(PATID)
);

