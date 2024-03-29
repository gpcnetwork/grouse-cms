/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: dispensing_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DISPENSING
*/

-- initialize table
create or replace table PRIVATE_DISPENSING (
	 DISPENSINGID varchar(19)
	,PATID varchar(50) NOT NULL
	,PRESCRIBINGID varchar(100) NULL
	,PRESCRIBERID varchar(19) NULL
    ,PROVIDERID varchar(19) NULL
	,DISPENSE_DATE date NOT NULL
	,NDC varchar (11) NOT NULL
	,DISPENSE_SUP number(18) NULL
	,DISPENSE_AMT number(18) NULL
	,DISPENSE_DOSE_DISP varchar(19) NULL
    ,DISPENSE_DOSE_DISP_UNIT  varchar(50) NULL
    ,DISPENSE_ROUTE  varchar(50) NULL
    ,DISPENSE_SOURCE varchar(2)
	,RAW_NDC varchar (50) NULL
	,RAW_RX_MED_NAME varchar(500)
	,RAW_DISPENSE_DOSE_DISP  varchar(50) NULL
    ,RAW_DISPENSE_DOSE_DISP_UNIT varchar(50) NULL
    ,RAW_DISPENSE_ROUTE varchar(50) NULL
    ,primary key (DISPENSINGID)
);
