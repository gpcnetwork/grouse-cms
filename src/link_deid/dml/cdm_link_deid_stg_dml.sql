/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: cdm_link_deid_stg_dml.sql                                                 
# Description: Staging crosswalk files to link CDM data with CMS data
*/

truncate bene_mapping.bene_xwalk_cms;
call link_deid_stg('CMS',1,'UNIQUE_BENE_XWALK_2022','UNIQUE_ID',NULL::string);

truncate patid_mapping.patid_xwalk_mu;
call link_deid_stg('MU',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_allina;
call link_deid_stg('ALLINA',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_ihc;
call link_deid_stg('IHC',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_kumc;
call link_deid_stg('KUMC',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_mcri;
call link_deid_stg('MCRI',1,NULL::string,NULL::string,'XWALK2CDM');

truncate patid_mapping.patid_xwalk_mcw;
call link_deid_stg('MCW',1,NULL::string,NULL::string,'XWALK2CDM');

truncate patid_mapping.patid_xwalk_uiowa;
call link_deid_stg('UIOWA',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_unmc;
call link_deid_stg('UNMC',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_uthouston;
call link_deid_stg('UTHOUSTON',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_uthscsa;
call link_deid_stg('UTHSCSA',1,NULL::string,NULL::string,'XWALK2CDM');

truncate patid_mapping.patid_xwalk_utsw;
call link_deid_stg('UTSW',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_uu;
call link_deid_stg('UU',1,NULL::string,NULL::string,NULL::string);

truncate patid_mapping.patid_xwalk_washu;
call link_deid_stg('WASHU',1,NULL::string,NULL::string,'XWALK2CDM');
