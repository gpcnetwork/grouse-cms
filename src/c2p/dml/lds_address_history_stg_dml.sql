/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: lds_address_history_stg_dml.sql                                                 
# Desccription: DML script for populating intermediate staging tables
*/

call stage_lds_address_history('MEDICARE_2011');
call stage_lds_address_history('MEDICARE_2012');
call stage_lds_address_history('MEDICARE_2013');
call stage_lds_address_history('MEDICARE_2014');
call stage_lds_address_history('MEDICARE_2015');
call stage_lds_address_history('MEDICARE_2016');
call stage_lds_address_history('MEDICARE_2017');

