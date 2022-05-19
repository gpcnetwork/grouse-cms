/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: demographic_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_DEMOGRAPHIC
# Caution: DDL script will re-initialize all relevant tables
*/
-- initialize final table
create or replace table PRIVATE_DEMOGRAPHIC (
     PATID varchar(50) NOT NULL -- BENE_ID
    ,BIRTH_DATE date NULL -- BENE_DOB
--    ,BIRTH_TIME varchar(5) NULL
    ,SEX varchar(2) NULL -- SEX
--    ,SEXUAL_ORIENTATION varchar(2) NULL
--    ,GENDER_IDENTITY varchar(2) NULL
    ,HISPANIC varchar(2) NULL -- RTI_RACE_CD (Enhanced race/ethnicity designation)
--    ,BIOBANK_FLAG varchar(1) DEFAULT 'N'
    ,RACE varchar(2) NULL --RTI_RACE_CD (Enhanced race/ethnicity designation)
--    ,PAT_PREF_LANGUAGE_SPOKEN varchar(3) NULL
    ,RAW_SEX varchar(50) NULL
--    ,RAW_SEXUAL_ORIENTATION varchar(50) NULL
--    ,RAW_GENDER_IDENTITY varchar(50) NULL
    ,RAW_HISPANIC varchar(50) NULL
    ,RAW_RACE varchar(50) NULL
--    ,RAW_PAT_PREF_LANGUAGE_SPOKEN varchar(50) NULL
    ,primary key (PATID)
);
