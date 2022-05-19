/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_LDS_ADDRESS_HISTORY
*/
-- initialize final table
create or replace table PRIVATE_LDS_ADDRESS_HISTORY ( 
    ADDRESSID VARCHAR(20 BYTE) NOT NULL, 
	PATID VARCHAR(50 BYTE) NOT NULL, 
	ADDRESS_USE VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_TYPE VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_PREFERRED VARCHAR(2 BYTE) NOT NULL, 
    ADDRESS_CITY VARCHAR(50 BYTE), 
    ADDRESS_COUNTY VARCHAR(50 BYTE), 
	ADDRESS_STATE VARCHAR(2 BYTE), 
	ADDRESS_ZIP5 VARCHAR(5 BYTE),  
	ADDRESS_ZIP9 VARCHAR(9 BYTE),
	ADDRESS_PERIOD_START DATE NOT NULL,
    ADDRESS_PERIOD_END DATE
);

