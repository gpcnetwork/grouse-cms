/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: dispensing_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DISPENSING_STAGE
*/

-- initialize staging table
use schema CMS_PCORNET_CDM_STAGING;
create or replace table PRIVATE_DISPENSING_STAGE (
     PDE_ID varchar(50) NOT NULL
	,BENE_ID varchar(20) NOT NULL
    ,PRSCRBID varchar(20) NOT NULL
    ,PRVDR_ID varchar(20)
    ,SRVC_DT date
	,PRDSRVID varchar(15)
	,DAYSSPLY integer
	,QTYDSPNS integer
    ,BN varchar(50)
    ,GNN varchar(50)
	,STR varchar(50)
	,GCDF varchar(50)
	,GCDF_DESC varchar(50)
	,SRC_SCHEMA varchar(20)
	,SRC_TABLE varchar(30)
);
