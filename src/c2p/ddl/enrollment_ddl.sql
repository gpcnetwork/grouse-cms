/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:      
# - PRIVATE_ENROLLMENT
# Caution: DDL script will re-initialize all relevant tables
*/

-- initialize final table
create or replace table PRIVATE_ENROLLMENT (
     PATID varchar(50) NOT NULL
    ,ENR_START_DATE date NOT NULL
	,ENR_END_DATE date NULL
	,CHART varchar(1) NULL
	,ENR_BASIS varchar(1) NOT NULL
--	,RAW_CHART varchar(50) NULL
	,RAW_BASIS varchar(50) NULL
);
