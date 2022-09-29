/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: main_transform.sql                                                 
# Desccription: main DML script for populating final transformed
#               CDM tables according to logical order 
*/

--enrollment--
call transform_to_enrollment('AB');
call transform_to_enrollment('C');
call transform_to_enrollment('D');

--demographic--
call transform_to_demographic('MEDICARE_2011');
call transform_to_demographic('MEDICARE_2012');
call transform_to_demographic('MEDICARE_2013');
call transform_to_demographic('MEDICARE_2014');
call transform_to_demographic('MEDICARE_2015');
call transform_to_demographic('MEDICARE_2016');
call transform_to_demographic('MEDICARE_2017');

--death--
call transform_to_death('MEDICARE_2011');
call transform_to_death('MEDICARE_2012');
call transform_to_death('MEDICARE_2013');
call transform_to_death('MEDICARE_2014');
call transform_to_death('MEDICARE_2015');
call transform_to_death('MEDICARE_2016');
call transform_to_death('MEDICARE_2017');

--lds_address_history--
call transform_to_lds_address_history('MEDICARE_2011');
call transform_to_lds_address_history('MEDICARE_2012');
call transform_to_lds_address_history('MEDICARE_2013');
call transform_to_lds_address_history('MEDICARE_2014');
call transform_to_lds_address_history('MEDICARE_2015');
call transform_to_lds_address_history('MEDICARE_2016');
call transform_to_lds_address_history('MEDICARE_2017');

--encounter--
call transform_to_encounter('MEDICARE_2011');
call transform_to_encounter('MEDICARE_2012');
call transform_to_encounter('MEDICARE_2013');
call transform_to_encounter('MEDICARE_2014');
call transform_to_encounter('MEDICARE_2015');
call transform_to_encounter('MEDICARE_2016');
call transform_to_encounter('MEDICARE_2017');

--diagnosis--
call transform_to_diagnosis('MEDICARE_2011');
call transform_to_diagnosis('MEDICARE_2012');
call transform_to_diagnosis('MEDICARE_2013');
call transform_to_diagnosis('MEDICARE_2014');
call transform_to_diagnosis('MEDICARE_2015');
call transform_to_diagnosis('MEDICARE_2016');
call transform_to_diagnosis('MEDICARE_2017');

--procedures--
call transform_to_procedures('MEDICARE_2011');
call transform_to_procedures('MEDICARE_2012');
call transform_to_procedures('MEDICARE_2013');
call transform_to_procedures('MEDICARE_2014');
call transform_to_procedures('MEDICARE_2015');
call transform_to_procedures('MEDICARE_2016');
call transform_to_procedures('MEDICARE_2017');

--dispensing--
call transform_to_dispensing('MEDICARE_2011'); 
call transform_to_dispensing('MEDICARE_2012');
call transform_to_dispensing('MEDICARE_2013');
call transform_to_dispensing('MEDICARE_2014');
call transform_to_dispensing('MEDICARE_2015');
call transform_to_dispensing('MEDICARE_2016');
call transform_to_dispensing('MEDICARE_2017');
