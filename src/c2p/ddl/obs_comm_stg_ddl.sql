/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_OBS_COMM
*/
-- initialize staging tables for each source
create or replace table PRIVATE_OBS_COMM_STAGE ( 
	 GEOCODEID VARCHAR(20) NOT NULL
    ,OBSCOMM_GEO_ACCURACY VARCHAR(20) NOT NULL
    ,OBSCOMM_CODE VARCHAR(20)
    ,OBSCOMM_TYPE VARCHAR(30)
    ,OBSCOMM_TYPE_QUAL VARCHAR(30)
    ,OBSCOMM_RESULT_TEXT VARCHAR(50)
    ,OBSCOMM_RESULT_NUM NUMBER(38,5)
    ,OBSCOMM_RESULT_MODIFIER VARCHAR(5) 
    ,OBSCOMM_RESULT_UNIT VARCHAR(20)
    ,RAW_OBSCOMM_NAME VARCHAR(1000)
    ,RAW_OBSCOMM_RESULT VARCHAR(100)
    ,SRC_SCHEMA varchar(20) NOT NULL
    ,SRC_TABLE varchar(30) NOT NULL
    ,SRC_DATE_START date NOT NULL
    ,SRC_DATE_END date NOT NULL
);





