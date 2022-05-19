/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: death_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DEATH_STAGE
*/

-- initialize staging table
create or replace table %s.PRIVATE_DEATH_STAGE (
     BENE_ID varchar(50) NOT NULL
    ,DEATH_DT date NULL
    ,V_DOD_SW varchar(1)
    ,SRC_SCHEMA varchar(20) NOT NULL
    ,SRC_TABLE varchar(30) NOT NULL
    ,SRC_DATE date NOT NULL
);
