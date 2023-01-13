/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_ddl.sql                                                 
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_ADDRESS_HISTORY
*/

-- initialize final table
create or replace table PRIVATE_ADDRESS_HISTORY ( 
    ADDRESSID VARCHAR(100) NOT NULL, 
	PATID VARCHAR(50) NOT NULL, 
	ADDRESS_USE VARCHAR(2), 
	ADDRESS_TYPE VARCHAR(2), 
	ADDRESS_PREFERRED VARCHAR(2), 
    ADDRESS_STREET VARCHAR(200),
    ADDRESS_DETAIL VARCHAR(100),
    ADDRESS_CITY VARCHAR(50), 
    ADDRESS_COUNTY VARCHAR(50), 
	ADDRESS_STATE VARCHAR(2), 
	ADDRESS_ZIP5 VARCHAR(5),  
	ADDRESS_ZIP9 VARCHAR(9),
	ADDRESS_PERIOD_START DATE,
    ADDRESS_PERIOD_END DATE,
    RAW_ADDRESS_TEXT VARCHAR(300)
);

