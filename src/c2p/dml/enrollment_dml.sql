/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_dml.sql                                                 
# Desccription: main DML script for populating final CDM tables
*/

call transform_to_enrollment('AB', %s);
call transform_to_enrollment('C', %s);
call transform_to_enrollment('D', %s);