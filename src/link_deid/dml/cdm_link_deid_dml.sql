/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid_dml.sql                                                 

safe harbor rule: 
1. random date shifting but consistent at individual level
2. mask birth_date of age > 89 to 1900-01-01
3. All geographic subdivisions smaller than a state, including street address, city, county, 
   precinct, ZIP code, and their equivalent geocodes (e.g., census tract, census block) should 
   either be removed or hashed

linkage process: 
1. add DOB to bene_mapping table and create DOB_DEID with birth_date masking
2. add random shift column SHIFT to bene_mapping and BENE_ID_HASH
3. create patid_mapping_<site-abbr> for each site with columns: PATID, PAT_DOB, PAT_DOB_DEID, BENE_ID_HASH, BENE_DOB, BENE_DOB_DEID, SHIFT
   3.1. if there is a match with bene_mapping, populate PATID_HASH as BENE_ID_HASH and inherit shift to patid_mapping, 
   3.2. otherwise, generate new PATID_HASH and random SHIFT to patid_mapping
4. create materialized views for all CDMs (cms and site) with aligned patid and add DOB_DEID, SHIFT at the end
*/

-- generate lds and de-id tables for cms cdm data
-- call link_deid('CMS', null::string);
-- call link_deid('CMS', 'PRIVATE_ADDRESS_HISTORY'); -- single table update
-- call link_deid('CMS','PRIVATE_ADDRESS_GEOCODE'); -- single table update
-- call link_deid('CMS','OBS_COMM'); -- single table update
-- call link_deid('CMS','ENROLLMENT'); -- single table update
call link_deid('CMS','PROCEDURES'); -- single table update

-- generate lds and de-id tables for site cdm
call link_deid('MU', null::string);
call link_deid('ALLINA', null::string);
call link_deid('IHC', null::string);
call link_deid('KUMC', null::string);
call link_deid('MCRI', null::string);
call link_deid('MCW', null::string);
call link_deid('UIOWA', null::string);
call link_deid('UNMC', null::string);
call link_deid('UTHOUSTON', null::string);
call link_deid('UTHSCSA', null::string);
call link_deid('UTSW', null::string);
call link_deid('UU', null::string);
call link_deid('WASHU', null::string);

-- generate secure share views for de-id tables
call gen_deid_view('CMS');
call gen_deid_view('MU');
call gen_deid_view('ALLINA');
call gen_deid_view('MCW');
call gen_deid_view('UTHSCSA');
call gen_deid_view('MCRI');
call gen_deid_view('UU');
call gen_deid_view('UTHOUSTON');
call gen_deid_view('UTSW');
call gen_deid_view('UNMC');
call gen_deid_view('WASHU');
call gen_deid_view('KUMC');
call gen_deid_view('IHC');
call gen_deid_view('UIOWA');
