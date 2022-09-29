/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid_dml.sql                                                 

safe harbor rule: 
1. random date shifting but consistent at individual level
2. mask birth_date of age > 89 to 1900-01-01

linkage process: 
1. add DOB to bene_mapping table and create DOB_DEID with birth_date masking
2. add random shift column SHIFT to bene_mapping and BENE_ID_HASH
3. create patid_mapping_<site-abbr> for each site with columns: PATID, PAT_DOB, PAT_DOB_DEID, BENE_ID_HASH, BENE_DOB, BENE_DOB_DEID, SHIFT
   3.1. if there is a match with bene_mapping, populate PATID_HASH as BENE_ID_HASH and inherit shift to patid_mapping, 
   3.2. otherwise, generate new PATID_HASH and random SHIFT to patid_mapping
4. create materialized views for all CDMs (cms and site) with aligned patid and add DOB_DEID, SHIFT at the end
*/


call link_deid('CMS');
call link_deid('MU');

call link_deid('ALLINA');
call link_deid('IHC');
call link_deid('KUMC');
call link_deid('MCRI');
call link_deid('MCW');
call link_deid('UIOWA');
call link_deid('UNMC');
call link_deid('UTHOUSTON');
call link_deid('UTHSCSA');
call link_deid('UTSW');
call link_deid('UU');
call link_deid('WASHU');



