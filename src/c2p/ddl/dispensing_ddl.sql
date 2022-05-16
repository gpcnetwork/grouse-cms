/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: dispensing_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DISPENSING
# - PRIVATE_DISPENSING_STAGE
*/

-- initialize table
create or replace table PRIVATE_DISPENSING (
	 DISPENSINGID varchar(19)
	,PATID varchar(50) NOT NULL
	,PRESCRIBINGID varchar(19) NULL
	,DISPENSE_DATE date NOT NULL
	,NDC varchar (11) NOT NULL
	,DISPENSE_SUP number(18) NULL
	,DISPENSE_AMT number(18) NULL
	,DISPENSE_DOSE_DISP number(18) NULL
    ,DISPENSE_DOSE_DISP_UNIT  varchar(50) NULL
    ,DISPENSE_ROUTE  varchar(50) NULL
    ,DISPENSE_SOURCE varchar(2)
	,RAW_NDC varchar (50) NULL
	,RAW_DISPENSE_DOSE_DISP  varchar(50) NULL
    ,RAW_DISPENSE_DOSE_DISP_UNIT varchar(50) NULL
    ,RAW_DISPENSE_ROUTE varchar(50) NULL
    ,primary key (DISPENSINGID)
);
-- initialize staging table
use schema CMS_PCORNET_CDM_STAGING;
create table PRIVATE_DISPENSING_STAGE (
     PDE_ID varchar(50) NOT NULL
	,BENE_ID varchar(20) NOT NULL
    ,PRSCRBID varchar(20) NOT NULL
    ,SRVC_DT date
	,PRDSRVID varchar(15)
	,DAYSSPLY integer
	,QTYDSPNS integer
	,STR varchar(50)
	,GCDF varchar(50)
	,GCDF_DESC varchar(50)
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);
