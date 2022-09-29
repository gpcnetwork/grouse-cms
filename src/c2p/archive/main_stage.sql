/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: main_stage.sql                                                 
# Desccription: main DML script for populating intermediate 
#               staging tables according to logical order 
*/

--enrollment--
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

--demographic--
call stage_demographic('MEDICARE_2011');
call stage_demographic('MEDICARE_2012');
call stage_demographic('MEDICARE_2013');
call stage_demographic('MEDICARE_2014');
call stage_demographic('MEDICARE_2015');
call stage_demographic('MEDICARE_2016');
call stage_demographic('MEDICARE_2017');

--death--
call stage_death('MEDICARE_2011');
call stage_death('MEDICARE_2012');
call stage_death('MEDICARE_2013');
call stage_death('MEDICARE_2014');
call stage_death('MEDICARE_2015');
call stage_death('MEDICARE_2016');
call stage_death('MEDICARE_2017');

--lds_address_history--
call stage_lds_address_history('MEDICARE_2011');
call stage_lds_address_history('MEDICARE_2012');
call stage_lds_address_history('MEDICARE_2013');
call stage_lds_address_history('MEDICARE_2014');
call stage_lds_address_history('MEDICARE_2015');
call stage_lds_address_history('MEDICARE_2016');
call stage_lds_address_history('MEDICARE_2017');

--encounter--
call stage_encounter('MEDICARE_2011',NULL::string);
call stage_encounter('MEDICARE_2012',NULL::string);
call stage_encounter('MEDICARE_2013',NULL::string);
call stage_encounter('MEDICARE_2014',NULL::string);
call stage_encounter('MEDICARE_2015',NULL::string);
call stage_encounter('MEDICARE_2016',NULL::string);
call stage_encounter('MEDICARE_2017',NULL::string);

--diagnosis--
call stage_diagnosis('MEDICARE_2011',NULL::string);
call stage_diagnosis('MEDICARE_2012',NULL::string);
call stage_diagnosis('MEDICARE_2013',NULL::string);
call stage_diagnosis('MEDICARE_2014',NULL::string);
call stage_diagnosis('MEDICARE_2015',NULL::string);
call stage_diagnosis('MEDICARE_2016',NULL::string);
call stage_diagnosis('MEDICARE_2017',NULL::string);

--procedures--
call stage_procedures('MEDICARE_2011',NULL::string);
call stage_procedures('MEDICARE_2012',NULL::string);
call stage_procedures('MEDICARE_2013',NULL::string);
call stage_procedures('MEDICARE_2014',NULL::string);
call stage_procedures('MEDICARE_2015',NULL::string);
call stage_procedures('MEDICARE_2016',NULL::string);
call stage_procedures('MEDICARE_2017',NULL::string);

--dispensing--
call stage_dispensing('MEDICARE_2011');
call stage_dispensing('MEDICARE_2012');
call stage_dispensing('MEDICARE_2013');
call stage_dispensing('MEDICARE_2014');
call stage_dispensing('MEDICARE_2015');
call stage_dispensing('MEDICARE_2016');
call stage_dispensing('MEDICARE_2017');

