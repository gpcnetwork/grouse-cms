#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: main_cdm.py                                                 
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
import pandas as pd

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
             'mcri' #xwalk
             ,'mcw' #xwalk
            #  ,'uiowa'
            #  ,'unmc'
            #  ,'uthouston'
             ,'uthscsa' #xwalk
            #  ,'utsw'
            #  ,'uu'
             ,'washu' #xwalk
            ]
            
#create snowflake connection context
snowflake_conn = load.SnowflakeConnection(user,pwd,acct)  
with snowflake_conn as conn:
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)

    # part I - write sas7bdat file from s3 bucket to snowflake
    # breakpoint - modify k if loop gets interrupted
    k = 0
    for site in gpc_list[k:]:
        # reconstruct source bucket name and target schema name
        src_bucket = f'gpc-{site}-upload'   # make sure the bucket/subfolder name structure is correct
        params["env_schema"] = f'PCORNET_CDM_{site.upper()}'
        
        # so that empty buckets won't break the process
        try:
            filenames = utils.get_objects(src_bucket,'extract')
        except Exception as e:
            print(f'Error:{e}')
            continue

        # identify crosswalk file 
        src_file = [x for x in filenames[''] if 'XWALK' in x.upper()]
        if len(src_file) == 0:
            continue # skip if not exists
        load.Download_S3Objects(f'{src_bucket}',f'extract/{src_file[0]}',src_file[0])
        df = pd.read_csv(src_file[0],header = 0,skiprows = 0)
        
        # clean up column names
        colnm = [x.upper() for x in df.columns]
        if set(colnm) != set(['HASHID','PATID']):
            colnm = [('HASHID' if ('HASH' in s) else 'PATID') for s in colnm]
        df.columns = colnm
        
        # upload file to snowflake
        params["tgt_table"] = 'XWALK2CDM'
        sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],colnm)
        conn.cursor().execute(sql_generator.GenerateDDL())
        load.SfWrite_PandaDF(conn,params,df)
        
        # clean up
        del df
        os.remove(src_file[0])
        
    utils.pyclean()