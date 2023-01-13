/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: address_geocode_dml.sql                                                 
# Description: DML script for populating final CDM tables; complete refresh
# Dependency: PRIVATE_ADDRESS_HISTORY up to date
*/

call transform_to_private_address_geocode('GEOID_MAPPING.Z9_TO_BG','ADDRESS_ZIP9');
