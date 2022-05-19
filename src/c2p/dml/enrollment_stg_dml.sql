/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: enrollment_stg_dml.sql                                                 
# Desccription: DML script for populating intermediate staging tables
*/

call stage_enrollment('MEDICARE_2011','AB'); 
call stage_enrollment('MEDICARE_2011','D'); 

call stage_enrollment('MEDICARE_2012','AB');
call stage_enrollment('MEDICARE_2012','D');

call stage_enrollment('MEDICARE_2013','AB');
call stage_enrollment('MEDICARE_2013','D');

call stage_enrollment('MEDICARE_2014','AB');
call stage_enrollment('MEDICARE_2014','C');
call stage_enrollment('MEDICARE_2014','D');

call stage_enrollment('MEDICARE_2015','AB');
call stage_enrollment('MEDICARE_2015','C');
call stage_enrollment('MEDICARE_2015','D');

call stage_enrollment('MEDICARE_2016','AB');
call stage_enrollment('MEDICARE_2016','C');
call stage_enrollment('MEDICARE_2016','D');

call stage_enrollment('MEDICARE_2017','AB');
call stage_enrollment('MEDICARE_2017','C');
call stage_enrollment('MEDICARE_2017','D');
