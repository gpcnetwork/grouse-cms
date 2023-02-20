#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: stage_cdm.py                                                 
# The file read Snowflake credential from secret manager and establish
# database connection using python connector; and send DML script 
# over to snowflake to perform data staging steps                                              
#####################################################################
# BEFORE YOU START, 
# a. INSTALL DEPENDENCIES BY RUNNING ./dep/setup.sh 
# b. MAKE SURE extract.py, load.py, utils.py ARE LOADED UNDER THE SAME DIRECTORY
#####################################################################
import os
import json
import utils
import load
import extract
import numpy as np
from re import match, sub

#diagnosic mode
diagnostic_mode = True

#if skip download
skip_download = False

#load by chunks
load_by_chunk = True
chunk_size = 10000000


#specify other snowflake parameters (not sensitive)
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

#extract snowflake secrets from secret manager
region = config_data["aws_grouse_default"]["region"]
user = load.AWSSecrets(region,secret_name = config_data["aws_grouse_default"]["user_secret"])
pwd = load.AWSSecrets(region,secret_name = config_data["aws_grouse_default"]["pwd_secret"])
acct = load.AWSSecrets(region,secret_name = config_data["aws_grouse_default"]["acct_secret"])
params = config_data["snowflake_cms_admin_default"]

#get other aws parameters
gpc_list = [  
            #  'allina'
            #  'ihc'
            #  'kumc'
            #  'mcri' #xwalk
            #  ,'mcw' #xwalk
            #  ,'uiowa'
              'unmc'
            #  ,'uthouston'
            #  ,'uthscsa' #xwalk
            #  ,'utsw'
            #  ,'uu'
            #  ,'washu' #xwalk
            ]
    
tbl_incld = [ 
              'harvest'
             ,'condition'
            #  ,'death_cause'
            #  ,'death'
            #  ,'demographic'
             ,'diagnosis'
            #  ,'dispensing'
            #  ,'encounter'
            #  ,'enrollment'
            #  ,'immunization'
            #  ,'lab_history'
            #  ,'lab_result_cm'
            #  ,'med_admin'
            #  ,'obs_clin'
            #  ,'obs_gen'
            #  ,'pcornet_trial'
            #  ,'prescribing'
            #  ,'pro_cm'
            #  ,'procedures'
            #  ,'provider'
            #  ,'vital'
            ]
            
#download pcornet cdm metadata
cdm_meta = utils.load_meta_pcornet_url( url = 'https://pcornet.org/wp-content/uploads/2021/11/2021_11_29_PCORnet_Common_Data_Model_v6dot0_parseable.xlsx',
                                        sheet = 'FIELDS',
                                        tbl_col = 'TABLE_NAME',
                                        var_col = 'FIELD_NAME',
                                        dtype_col = 'SAS_DATA_TYPE',
                                        exclude_raw_col=True)
#create snowflake connection context
snowflake_conn = load.SnowflakeConnection(user,pwd,acct)  
with snowflake_conn as conn:
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)

    # initialize benchmark params
    benchmk_data = []
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),"benchmark","benchmark_staging.csv")
    
    # part I - write sas7bdat file from s3 bucket to snowflake
    # breakpoint - modify k if loop gets interrupted
    k = 0
    for site in gpc_list[k:]:
        # reconstruct source bucket name and target schema name
        src_bucket = f'gpc-{site}-upload'   # make sure the bucket/subfolder name structure is correct
        params["env_schema"] = f'PCORNET_CDM_{site.upper()}'
        
        #=====================================================
        if diagnostic_mode: print(site) 
        #=====================================================
        
        # so that empty buckets won't break the process
        try:
            filenames = utils.get_objects(src_bucket,'extract')
        except Exception as e:
            print(f'Error:{e}')
            continue
        
        # start load .sas7bdat files
        try: 
            # if no subfolder, output dict of get_object will have key ''
            for idx, val in enumerate(filenames['']):
                #---simple benchmark--start
                start = utils.get_benchmark()
                #-----------------------------------
                
                #---main process---
                # identify target table name
                src_file_name = val.split('.')[0]
                src_file_type = val.split('.')[1]
                # file name may contain prefix and/or suffix
                # exception: death and death_cause are two different tables
                if not any(ele in src_file_name and f'{ele}_cause' not in src_file_name for ele in tbl_incld):
                    continue
                params["tgt_table"] = [x for x in tbl_incld if x in src_file_name][0].upper()
                tgt_table_full = params["tgt_table"]
                
                #=====================================================
                if diagnostic_mode: print(tgt_table_full) 
                #=====================================================
                
                # make sure it is the .sas7bdat file
                if not val.endswith('.sas7bdat'):
                    print(f'non-parsable file type: {src_file_type}')
                    continue
                
                # download file in full - once per file
                if not skip_download:
                    load.Download_S3Objects(src_bucket,
                                            f'extract/{src_file_name}.{src_file_type}',
                                            f'{src_file_name}.{src_file_type}')
                
                # write .sas7bdat to table
                if load_by_chunk:
                    # write by chuncks
                    chunk_idx = 1 # modifiable if break
                    skip_row = (chunk_idx-1)*chunk_size
                    next_row = True
                    
                    while next_row:
                        # read sas7bdat file by chunk
                        next_row, sasdf, sasmeta = load.Read_SAS7bDAT(src_file_name, 
                                                                      row_offset=skip_row,
                                                                      row_limit=chunk_size,
                                                                      encoding = 'iso-8859-1') #default is usually "utf-8". Alternatives: "latin1","iso-8859-1"
                        #---simple benchmark---midpoint---
                        mid = utils.get_benchmark()
                        #----------------------------------

                        # adjust metadata to accommodate for site differences
                        meta_adj = utils.amend_metadata(cdm_meta[tgt_table_full],sasmeta) 
                            
                        #=====================================================
                        if diagnostic_mode: print(meta_adj) 
                        #=====================================================
                
                        # create table shell on snowflake
                        params["tgt_table"] = f'{tgt_table_full}_{chunk_idx}'
                        sql_generator = extract.SqlGenerator_PcornetURL(params["env_schema"],params["tgt_table"],meta_adj)
                        conn.cursor().execute(sql_generator.GenerateDDL())
                        
                        # write pandas df to snowflake
                        try:
                            load.SfWrite_PandaDF(conn,params,sasdf)
                            del sasdf # clean memory
                        except: 
                            print('empty table is skipped!')
                        
                        # next iteration
                        skip_row += chunk_size
                        chunk_idx += 1
                else:
                    # read sas7bdat file by chunk
                    next_row, sasdf, sasmeta = load.Read_SAS7bDAT(src_file_name,
                                                                  encoding = 'utf-8') #default is usually "utf-8". Alternatives: "latin1","iso-8859-1"
                    
                    #---simple benchmark---midpoint
                    mid = utils.get_benchmark()
                    #----------------------------------
                                                             
                    # create table shell on snowflake
                    sql_generator = extract.SqlGenerator_PcornetURL(params["tgt_schema"],params["tgt_table"],meta_adj)
                    conn.cursor().execute(sql_generator.GenerateDDL())
                    
                    #=====================================================
                    if diagnostic_mode: print(params["tgt_table"]) 
                    #=====================================================
                    
                    # write panda df to snowflake
                    load.SfWrite_PandaDF(conn,params,sasdf)
                    del sasdf
                    
                #---delete sas file from disk and release memory
                os.remove(val)
            
                #---simple benchmark---end
                end = [max(a,b) for a,b in zip(mid, utils.get_benchmark())] 
                delta = [b-a for a, b in zip(start, end)]
                benchmk_data.append([site,val,delta])
                #------------------------------------------------------------------
                
                #---report progress
                print(f'{site}...{val}...loaded onto snowflake in {round(delta[0],2)} seconds')
                
                #---write benchmark result to .csv
                np.savetxt(file_path, benchmk_data, delimiter = ',',fmt="%s")

        except Exception as e:
            print(f'Error:{e}')
    # part I ends
    
    # part II - stitch parts together if files are loaded in chunks
    k = 0
    for site in gpc_list[k:]:
        for tbl in tbl_incld:
            #---simple benchmark-- start
            start = utils.get_benchmark()
            #--------------------------------------------
            
            #---main process---
            parts = conn.cursor().execute(f"select distinct TABLE_SCHEMA||'.'||TABLE_NAME from INFORMATION_SCHEMA.TABLES " +
                                          f"where TABLE_SCHEMA like '%{site.upper()}' and TABLE_NAME rlike '{tbl.upper()}_*[0-9]+$'").fetchall()
            if not parts:
                continue
            else: 
                parts = [x[0] for x in parts]
                tbl_full = parts[0].split('.')[0] +'.' + tbl.upper()
                load.SfExec_StitchParts(conn.cursor(),tbl_full,parts,
                                        drop_after_merge=True)
            
            #---simple benchmark-- end
            end = utils.get_benchmark()
            delta = [b-a for a, b in zip(start, end)]
            benchmk_data.append([site,F'{tbl}_stitch',delta])
            #---------------------------------------------------------------
            
            #---report progress
            print(f'{site}...{tbl}...stitched in {round(delta[0],2)} seconds')
            
            #---write benchmark result to .csv
            np.savetxt(file_path, benchmk_data, delimiter = ',',fmt="%s")
    # part II ends
    
    utils.pyclean()
    