/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: procedures_stg_dml.sql                                                 
# Desccription: DML script for populating intermediate staging tables
*/

call stage_procedures('MEDICARE_2011',NULL::string);
call stage_procedures('MEDICARE_2012',NULL::string);
call stage_procedures('MEDICARE_2013',NULL::string);
call stage_procedures('MEDICARE_2014',NULL::string);
call stage_procedures('MEDICARE_2015',NULL::string);
call stage_procedures('MEDICARE_2016',NULL::string);
call stage_procedures('MEDICARE_2017',NULL::string);
