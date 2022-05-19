/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: demographic_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DEMOGRAPHIC_STAGE
# Caution: DDL script will re-initialize all relevant tables
*/

-- initialize staging table
create or replace table PRIVATE_DEMOGRAPHIC_STAGE (
     BENE_ID varchar(50) NOT NULL
    ,BENE_DOB date NOT NULL
    ,SEX varchar(10) NULL
    ,RACE varchar(10) NULL
    ,RTI_RACE_CD varchar(10) NULL
    ,SRC_SCHEMA varchar(20) NOT NULL
    ,SRC_TABLE varchar(30) NOT NULL
    ,SRC_DATE date NOT NULL
);
