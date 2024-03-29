/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:      
# - PRIVATE_ENROLLMENT_AB_STAGE
# - PRIVATE_ENROLLMENT_C_STAGE
# - PRIVATE_ENROLLMENT_D_STAGE
# Caution: DDL script will re-initialize all relevant tables
*/

-- initialize staging tables
create or replace table PRIVATE_ENROLLMENT_STAGE_AB (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    OREC VARCHAR(2),
	CREC VARCHAR(2),
    BUYIN01 varchar(1) NULL,
    BUYIN02 varchar(1) NULL,
    BUYIN03 varchar(1) NULL, 
    BUYIN04 varchar(1) NULL,
    BUYIN05 varchar(1) NULL, 
    BUYIN06 varchar(1) NULL,
    BUYIN07 varchar(1) NULL,
    BUYIN08 varchar(1) NULL,
    BUYIN09 varchar(1) NULL,
    BUYIN10 varchar(1) NULL,
    BUYIN11 varchar(1) NULL,
    BUYIN12 varchar(1) NULL,
    HMOIND01 varchar(1) NULL,
    HMOIND02 varchar(1) NULL,
    HMOIND03 varchar(1) NULL,
    HMOIND04 varchar(1) NULL,
    HMOIND05 varchar(1) NULL,
    HMOIND06 varchar(1) NULL,
    HMOIND07 varchar(1) NULL,
    HMOIND08 varchar(1) NULL,
    HMOIND09 varchar(1) NULL,
    HMOIND10 varchar(1) NULL,
    HMOIND11 varchar(1) NULL,
    HMOIND12 varchar(1) NULL,
    SRC_SCHEMA varchar(20),
    SRC_TABLE varchar(20),
    SRC_DATE date NOT NULL
);

create or replace table PRIVATE_ENROLLMENT_STAGE_C (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    OREC VARCHAR(2),
	CREC VARCHAR(2),
    PTCCNTRCT01 varchar(5) NULL,
    PTCCNTRCT02 varchar(5) NULL,
    PTCCNTRCT03 varchar(5) NULL,
    PTCCNTRCT04 varchar(5) NULL,
    PTCCNTRCT05 varchar(5) NULL,
    PTCCNTRCT06 varchar(5) NULL,
    PTCCNTRCT07 varchar(5) NULL,
    PTCCNTRCT08 varchar(5) NULL,
    PTCCNTRCT09 varchar(5) NULL,
    PTCCNTRCT10 varchar(5) NULL,
    PTCCNTRCT11 varchar(5) NULL,
    PTCCNTRCT12 varchar(5) NULL,
    PTCPBPID01 varchar(3) NULL,
    PTCPBPID02 varchar(3) NULL,
    PTCPBPID03 varchar(3) NULL,
    PTCPBPID04 varchar(3) NULL,
    PTCPBPID05 varchar(3) NULL,
    PTCPBPID06 varchar(3) NULL,
    PTCPBPID07 varchar(3) NULL,
    PTCPBPID08 varchar(3) NULL,
    PTCPBPID09 varchar(3) NULL,
    PTCPBPID10 varchar(3) NULL,
    PTCPBPID11 varchar(3) NULL,
    PTCPBPID12 varchar(3) NULL,
    SRC_SCHEMA varchar(20),
    SRC_TABLE varchar(20),
    SRC_DATE date NOT NULL
);

create or replace table PRIVATE_ENROLLMENT_STAGE_D (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    PTDCNTRCT01 varchar(5) NULL,
    PTDCNTRCT02 varchar(5) NULL,
    PTDCNTRCT03 varchar(5) NULL,
    PTDCNTRCT04 varchar(5) NULL,
    PTDCNTRCT05 varchar(5) NULL,
    PTDCNTRCT06 varchar(5) NULL,
    PTDCNTRCT07 varchar(5) NULL,
    PTDCNTRCT08 varchar(5) NULL,
    PTDCNTRCT09 varchar(5) NULL,
    PTDCNTRCT10 varchar(5) NULL,
    PTDCNTRCT11 varchar(5) NULL,
    PTDCNTRCT12 varchar(5) NULL,
    PTDPBPID01 varchar(3) NULL,
    PTDPBPID02 varchar(3) NULL,
    PTDPBPID03 varchar(3) NULL,
    PTDPBPID04 varchar(3) NULL,
    PTDPBPID05 varchar(3) NULL,
    PTDPBPID06 varchar(3) NULL,
    PTDPBPID07 varchar(3) NULL,
    PTDPBPID08 varchar(3) NULL,
    PTDPBPID09 varchar(3) NULL,
    PTDPBPID10 varchar(3) NULL,
    PTDPBPID11 varchar(3) NULL,
    PTDPBPID12 varchar(3) NULL,
    RDSIND01 varchar(1) NULL,
    RDSIND02 varchar(1) NULL,
    RDSIND03 varchar(1) NULL,
    RDSIND04 varchar(1) NULL,
    RDSIND05 varchar(1) NULL,
    RDSIND06 varchar(1) NULL,
    RDSIND07 varchar(1) NULL,
    RDSIND08 varchar(1) NULL,
    RDSIND09 varchar(1) NULL,
    RDSIND10 varchar(1) NULL,
    RDSIND11 varchar(1) NULL,
    RDSIND12 varchar(1) NULL,
    SRC_SCHEMA varchar(20),
    SRC_TABLE varchar(20),
    SRC_DATE date NOT NULL
);

/*staging table for medicaid dual eligibility*/
create or replace table PRIVATE_ENROLLMENT_STAGE_DUAL (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    DUAL_01 varchar(5) NULL,
    DUAL_02 varchar(5) NULL,
    DUAL_03 varchar(5) NULL,
    DUAL_04 varchar(5) NULL,
    DUAL_05 varchar(5) NULL,
    DUAL_06 varchar(5) NULL,
    DUAL_07 varchar(5) NULL,
    DUAL_08 varchar(5) NULL,
    DUAL_09 varchar(5) NULL,
    DUAL_10 varchar(5) NULL,
    DUAL_11 varchar(5) NULL,
    DUAL_12 varchar(5) NULL,
    SRC_SCHEMA varchar(20),
    SRC_TABLE varchar(20),
    SRC_DATE date NOT NULL
);

/*staging table for low-income-subsidity eligibility*/
create or replace table PRIVATE_ENROLLMENT_STAGE_LIS (
    BENE_ID varchar(50) NOT NULL,
    RFRNC_YR varchar(5) NOT NULL,
    CSTSHR01 varchar(5) NULL,
    CSTSHR02 varchar(5) NULL,
    CSTSHR03 varchar(5) NULL,
    CSTSHR04 varchar(5) NULL,
    CSTSHR05 varchar(5) NULL,
    CSTSHR06 varchar(5) NULL,
    CSTSHR07 varchar(5) NULL,
    CSTSHR08 varchar(5) NULL,
    CSTSHR09 varchar(5) NULL,
    CSTSHR10 varchar(5) NULL,
    CSTSHR11 varchar(5) NULL,
    CSTSHR12 varchar(5) NULL,
    SRC_SCHEMA varchar(20),
    SRC_TABLE varchar(20),
    SRC_DATE date NOT NULL
);
