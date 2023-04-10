#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: stage_hashtoken.py                                                 
# After data is properly staged, we want to move                                        
#####################################################################

import utils 
import load
import pandas as pd
import csv

#diagnosic mode
diagnostic_mode = False

#if skip download
skip_download = False

#if skip unzip and copy from source/ to extract/
skip_copy = True

#loop over gpc sites
gpc_list = [  
              'mu'
            #  ,'allina'
            #  ,'ihc'
            #  ,'kumc'
            #  ,'mcri'
            #  ,'mcw'
            #  ,'uiowa'
            #  ,'unmc'
            #  ,'uthouston'
            #  ,'uthscsa'
            #  ,'utsw'
            #  ,'uu'
            #  ,'washu'
            ]
            
for idx, site in enumerate(gpc_list):
    # reconstruct source bucket name and target schema name
    src_bucket = f'gpc-{site}-upload'   # make sure the bucket/subfolder name structure is correct
    src_prefix = 'va-linkage-pilot/source'
    
    # identify file
    src_objs = utils.get_objects(bucket_name=src_bucket,subfolder=src_prefix)
    max_mod_date = max(src_objs['modified_date'])
    max_pos = src_objs['modified_date'].index(max_mod_date)
    src_file = src_objs[''][max_pos]
    src_key = f'{src_prefix}/{src_file}'
    
    #=====================================================
    if diagnostic_mode: print(src_key,":",max_mod_date)
    #=====================================================

    tgt_prefix = 'va-linkage-pilot/extract/'
    if not skip_copy:
        if '.zip' in src_file:
            # unzip file and save to target location
            utils.unzip_file(
                src_bucket = src_bucket, src_key = src_key,
                tgt_bucket = src_bucket, tgt_prefix = tgt_prefix    
            )
        else:
            # copy over to target location
            utils.file_to_folder(
                bucket_name = src_bucket,
                src_file = src_key,
                tgt_folder = tgt_prefix)

    # load unzipped csv file from disk
    if not skip_download:
        tgt_key = f'{tgt_prefix}{src_file}'
        load.Download_S3Objects(src_bucket,tgt_key,src_file)
    df = pd.read_csv(src_file,header = 0,skiprows = 0)
    
    #=====================================================
    if diagnostic_mode: print(df.head()); print(df.columns)
    #=====================================================
    
    # attach siteid
    side_idx = "{:02d}".format(gpc_list.index(site))
    df['SITEID'] = f'S{side_idx}'
    
    #=====================================================
    if diagnostic_mode: print(df.head()); print(df.columns)
    #=====================================================
    
    # save to target s3 bucket
    #https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
    tgt2_key_loc = f'gpc-va-hashtoken-{max_mod_date}.csv'
    # df.to_csv(tgt2_key,index=False)
    # with open(tgt2_key_loc, 'a') as f:
    #     #https://stackoverflow.com/questions/30991541/pandas-write-csv-append-vs-write
    #     df.to_csv(f, mode='a', header=f.tell()==0)
    load.Upload_S3Objects(
        file_to_save = df,
        bucket_name = 'va-download',
        tgt_key = tgt2_key_loc
    )