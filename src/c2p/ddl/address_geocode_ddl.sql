/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: address_geocode_ddl.sql                                             
# DDL script for initializing intermediate and resulting tables:                                               
# - PRIVATE_ADDRESS_GEOCODE
*/

-- initialize final table
create or replace table PRIVATE_ADDRESS_GEOCODE( 
     GEOCODEID VARCHAR(100) NOT NULL
    ,ADDRESSID VARCHAR(100) NOT NULL
    ,GEOCODE_STATE VARCHAR(2) 
    ,GEOCODE_COUNTY VARCHAR(5) 
	,GEOCODE_LONGITUDE VARCHAR(50)
    ,GEOCODE_LATITUDE VARCHAR(50)
    ,GEOCODE_TRACT VARCHAR(11)
    ,GEOCODE_GROUP VARCHAR(12)
    ,GEOCODE_BLOCK VARCHAR(15)
    ,GEOCODE_ZIP5 VARCHAR(5)
    ,GEOCODE_ZIP9 VARCHAR(10)
    ,GEOCODE_ZCTA VARCHAR(6)
    ,GEOCODE_CUSTOM VARCHAR(20)
    ,GEOCODE_CUSTOM_TEXT VARCHAR(20)
    ,SHAPEFILE VARCHAR(10)
    ,GEO_ACCURACY VARCHAR(2)
    ,GEO_PROV_REF VARCHAR(500)      
    ,ASSIGNMENT_DATE DATE        
);
