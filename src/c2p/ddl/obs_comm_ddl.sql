/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_OBS_COMM
*/
-- initialize final table
create or replace table OBS_COMM ( 
    GEOID VARCHAR(20) NOT NULL,
    GEOID_TYPE VARCHAR(20) NOT NULL,
    OBSCOMM_CODE VARCHAR(20),
    OBSCOMM_TYPE VARCHAR(30),
    OBSCOMM_TYPE_QUAL VARCHAR(30),
    OBSCOMM_RESULT_TEXT VARCHAR(50),
    OBSCOMM_RESULT_NUM NUMBER(38,5),
    OBSCOMM_RESULT_MODIFIER VARCHAR(5), 
    OBSCOMM_RESULT_UNIT VARCHAR(20),
    RAW_OBSCOMM_RESULT VARCHAR(100),
    OBSCOMM_START_DATE DATE,
    OBSCOMM_END_DATE DATE
);

