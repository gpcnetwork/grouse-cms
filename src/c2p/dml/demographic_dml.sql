/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: demographic_dml.sql                                                 
# Desccription: DML script for populating final CDM tables
*/

call transform_to_demographic('MEDICARE_2011');
call transform_to_demographic('MEDICARE_2012');
call transform_to_demographic('MEDICARE_2013');
call transform_to_demographic('MEDICARE_2014');
call transform_to_demographic('MEDICARE_2015');
call transform_to_demographic('MEDICARE_2016');
call transform_to_demographic('MEDICARE_2017');
