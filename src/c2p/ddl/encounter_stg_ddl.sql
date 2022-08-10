/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: encounter_stg_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_ENCOUNTER_STAGE_MEDPAR
# - PRIVATE_ENCOUNTER_STAGE_OUTPATIENT
# - PRIVATE_ENCOUNTER_STAGE_HHA
# - PRIVATE_ENCOUNTER_STAGE_HOSPICE
# - PRIVATE_ENCOUNTER_STAGE_BCARRIER
# - PRIVATE_ENCOUNTER_STAGE_DME
*/

-- initialize staging tables (multiple sources)
create or replace table PRIVATE_ENCOUNTER_STAGE_MEDPAR (
     BENE_ID varchar(20)
    ,MEDPARID varchar(20)
    ,TYPE_ADM varchar(3)
    ,SSLSSNF varchar(1)
    ,ADMSNDT date
    ,CVRLVLDT date
    ,DSCHRGDT date
    ,QLFYTHRU date
    ,ORGNPINM varchar(20)
    ,PRVDRNUM varchar(10)
    ,DSCHRGCD varchar(2)
    ,DSTNTNCD varchar(3)
    ,SRC_ADMS varchar(2)
    ,DRG_CD varchar(6)
    ,PRPAY_CD varchar(2)
    ,MT_ENC_TYPE varchar(5)
    ,MT_DISCHARGE_DATE date
    ,MT_FACILITY_TYPE varchar(50)
    ,MT_DISCHARGE_DISPOSITION varchar(5)
    ,MT_DISCHARGE_STATUS varchar(5)
    ,MT_ADMITTING_SOURCE varchar(5)
    ,MT_PAYER_TYPE_PRIMARY varchar(5)
    ,MT_PAYER_TYPE_SECONDARY varchar(5)
    ,SRC_SCHEMA varchar(20)
    ,SRC_TABLE varchar(30)
    ,DEDUP_INDEX integer
    ,primary key (BENE_ID, MEDPARID)
);

create or replace table PRIVATE_ENCOUNTER_STAGE_OUTPATIENT (
     BENE_ID varchar(20)
    ,CLM_ID varchar(20)
    -- ,REV_CNTR varchar(5)
    ,FROM_DT date
    ,THRU_DT date
    ,ORGNPINM varchar(12)
    ,PRSTATE varchar(3)
    -- ,SRVC_LOC_NPI_NUM varchar(12) -- doesn't exists prior to 2014
    ,AT_NPI varchar(12)
    -- ,AT_PHYSN_SPCLTY_CD varchar(3)
    ,OP_NPI varchar(12)
    -- ,OP_PHYSN_SPCLTY_CD varchar(3)
    ,OT_NPI varchar(12) 
    -- ,OT_PHYSN_SPCLTY_CD varchar(3)
    -- ,RNDRNG_PHYSN_NPI varchar(12)  -- doesn't exists prior to 2014
    -- ,RFR_PHYSN_NPI varchar(12)  -- doesn't exists prior to 2014
    ,PROVIDER varchar(10)
    ,FAC_TYPE varchar(2)
    ,TYPESRVC varchar(2)
    ,STUS_CD varchar(3)
    ,PRPAY_CD varchar(2) 
    ,MT_ENC_TYPE varchar(5)
    ,MT_DISCHARGE_DATE date
    ,MT_FACILITY_TYPE varchar(50)
    ,MT_DISCHARGE_STATUS varchar(5)
    ,MT_PAYER_TYPE_PRIMARY varchar(5)
    ,MT_PAYER_TYPE_SECONDARY varchar(5)
    ,SRC_SCHEMA varchar(20)
    ,SRC_TABLE varchar(30)
    ,DEDUP_INDEX integer
    ,primary key (BENE_ID, CLM_ID)
);

create or replace table PRIVATE_ENCOUNTER_STAGE_HHA (
     BENE_ID varchar(20)
    ,CLM_ID varchar(20)
    ,CLM_TYPE varchar(3)
    ,FROM_DT date
    ,THRU_DT date
    ,ORGNPINM varchar(12)
    -- ,CLM_SRVC_FAC_ZIP_CD varchar(12) -- doesn't exists prior to 2014
    ,PRSTATE varchar(3)
    -- ,SRVC_LOC_NPI_NUM varchar(12) -- doesn't exists prior to 2014
    ,AT_NPI varchar(12)
    -- ,AT_PHYSN_SPCLTY_CD varchar(3) -- doesn't exists prior to 2014
    -- ,OP_NPI varchar(12) -- doesn't exists prior to 2014
    -- ,OT_NPI varchar(12) -- doesn't exists prior to 2014
    -- ,RNDRNG_PHYSN_NPI varchar(12) -- doesn't exists prior to 2014
    -- ,RFR_PHYSN_NPI varchar(12) -- doesn't exists prior to 2014
    ,PROVIDER varchar(10)
    ,FAC_TYPE varchar(2)
    ,TYPESRVC varchar(2)
    ,STUS_CD varchar(3)
    ,PRPAY_CD varchar(2) 
    ,MT_ENC_TYPE varchar(5)
    ,MT_FACILITY_TYPE varchar(50)
    ,MT_DISCHARGE_STATUS varchar(5)
    ,MT_PAYER_TYPE_PRIMARY varchar(5)
    ,MT_PAYER_TYPE_SECONDARY varchar(5)
    ,SRC_SCHEMA varchar(20)
    ,SRC_TABLE varchar(30)
    ,DEDUP_INDEX integer
    ,primary key (BENE_ID, CLM_ID)
);
create or replace table PRIVATE_ENCOUNTER_STAGE_HOSPICE like PRIVATE_ENCOUNTER_STAGE_HHA;


create or replace table PRIVATE_ENCOUNTER_STAGE_BCARRIER (
     BENE_ID varchar(20)
    ,CLM_ID varchar(20)
    ,PLCSRVC varchar(3)
    ,THRU_DT date
    ,PRF_NPI varchar(12)
    ,PRGRPNPI varchar(12) -- usually empty
    ,RFR_NPI varchar(12) -- from header file (unique match)
    ,PROVZIP varchar(12)
    ,PRVSTATE varchar(3)
    ,HCFASPCL varchar(3)
    ,TYPSRVCB varchar(2)
    -- ,CPO_ORG_NPI_NUM varchar(12)  -- from header file (unique match), not exists prior to 2014
    ,LPRPAYCD varchar(2) 
    ,MT_ENC_TYPE varchar(5)
    ,MT_FACILITY_TYPE varchar(50)
    ,MT_PAYER_TYPE_PRIMARY varchar(5)
    ,MT_PAYER_TYPE_SECONDARY varchar(5)
    ,SRC_SCHEMA varchar(20)
    ,SRC_TABLE varchar(30)
    ,DEDUP_INDEX integer
    ,primary key (BENE_ID, CLM_ID)
);

create or replace table PRIVATE_ENCOUNTER_STAGE_DME (
     BENE_ID varchar(20)
    ,CLM_ID varchar(20)
    ,PLCSRVC varchar(3)
    ,THRU_DT date
    ,SUP_NPI varchar(12)
    ,RFR_NPI varchar(12) -- from header file (unique match)  
    ,PRVSTATE varchar(3)
    ,HCFASPCL varchar(3)
    ,TYPSRVCB varchar(2)
    ,LPRPAYCD varchar(2) 
    ,MT_ENC_TYPE varchar(5)
    ,MT_FACILITY_TYPE varchar(50)
    ,MT_PAYER_TYPE_PRIMARY varchar(5)
    ,MT_PAYER_TYPE_SECONDARY varchar(5)
    ,SRC_SCHEMA varchar(20)
    ,SRC_TABLE varchar(30)
    ,DEDUP_INDEX integer
    ,primary key (BENE_ID, CLM_ID)
);
