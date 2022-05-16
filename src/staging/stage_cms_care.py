#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: stage_cms_care.py                                                 
# The file read Snowflake credential from secret manager and establish
# database connection using python connector; and send DML script 
# over to snowflake to perform data staging steps                                              
#####################################################################
# BEFORE YOU START, 
# a. INSTALL DEPENDENCIES BY RUNNING ./dep/setup.sh 
# b. MAKE SURE extract.py, load.py, utils.py ARE LOADED UNDER THE SAME DIRECTORY
#####################################################################
import os
from utils import get_objects
from extract import *
from load import *
from re import match, sub

"""
within snowflake connection context, perform data staging process
1. Create a single-column fixed-width table
2. Copy .dat file from established snowflake "Stage" into single-column table shell
3. Construct DDL and "Substr" statements using "FTSParser" class and execute it in snowflake 
ref: https://docs.snowflake.com/en/user-guide/data-load-external-tutorial.html
"""

#get snowflake connections strings
user = AWSSecrets(secret_name = "snowflake-grouse-user")
pwd = AWSSecrets(secret_name = "snowflake-grouse-pwd")
acct = AWSSecrets(secret_name = "snowflake-grouse-acct")
    
#specify other snowflake parameters (not sensitive)
params = SnowflakeParams()
params.env_role = "GROUSE_ROLE_B_ADMIN"
params.env_wh = "GROUSE_WH"
params.env_db = "GROUSE_DB"
params.stg_stage = 'S3_EXT_STAGE' #need to modify for prod
params.stg_table = 'STAGE_TABLE'

#get other aws parameters
s3_bucket = "nextgenbmi-snowpipe-master" #may need to modify for prod
s3_key = "CMS/dat_files"                    #may need to modify for prod
filenames = get_objects(s3_bucket)

#mapping to fts file index
map_fts = [i for  y in filenames[f"{s3_subfolder}dat_files"] for i, x in enumerate(filenames[f"{s3_subfolder}fts_files"]) if match(x.split('.')[0],y.split('.')[0])]

#create snowflake connection context
snowflake_conn = SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # set up the snowflake environment
    SfExec_EnvSetup(conn.cursor(),params)
    SfExec_CreateFixedWidthTable(conn.cursor(),params)
    
    # initialize benchmark params
    benchmk_data = []
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),"benchmark","benchmark_staging.csv")
    
    # breakpoint - modify k if loop gets interrupted
    k = 0
    for idx, val in enumerate(filenames[s3_key][k:]):
        #---simple benchmark---start
        start = time.time() 
        
        # parse out metadata info and generate ddl and dml scripts
        fts_filename = filenames[f"{s3_subfolder}fts_files"][map_fts[(idx+k)]]
        filefts = s3open(f's3://{s3_bucket}/{s3_subfolder}fts_files/{fts_filename}','r')
        file_name = val.split('.')[0]
        fts_parse_out = FTSParser(filefts).parse_body()
        
        # automatically generate SQL scripts
        sql_generator = SqlGenerator_FTS(file_name,fts_parse_out,params.stg_table)

        # copy data file to STAGE_TABLE 
        SfExec_CopyIntoDat(conn.cursor(),params,file_name)
        
        '''
        #==== an OLTP-like approach
        # send DDL over to create expected table shell
        conn.cursor().execute(sql_generator.GenerateDDL())
        
        # send DML over to map single-colum staging table to STAGE_TABLE with whilespace normalization
        conn.cursor().execute(sql_generator.GenerateDML())
        '''
        #==== an OLAP-like approach
        # send DDL+DML over to create expected table shell
        conn.cursor().execute(sql_generator.GenerateDDML())
        
        #---simple benchmark-- end
        end = time.time()
        delta = round(end - start)
        benchmk_data.append([val,
                             ','.join(str(i) for i in fts_parse_out["colsize"]),
                             ','.join(str(i) for i in fts_parse_out["rowsize"])+':'+','.join(str(i) for i in fts_parse_out["rowsize_part"]),
                             ','.join(str(i) for i in fts_parse_out["filesize"]),
                             delta])
        
        #---stepwise report
        print(f'file {idx+k} completed: finish staging file {val} in {delta} secs')
        
        #---write benchmark result to .csv
        np.savetxt(file_path, benchmk_data, delimiter = ',',fmt="%s")
    
    #collect multi-part mapping
    ungroup_map = {}
    for fname in filenames[f"{s3_subfolder}dat_files"]:
        fname_obj = extract.FileNameParser(fname.split('.')[0])
        if(match('.*_[0-9]{3}$',fname_obj.GetTableName)):
            ParentTable = sub('_[0-9]{3}$','',fname_obj.GetTableName)
            ungroup_map[f'{fname_obj.GetSchemaName}.{fname_obj.GetTableName}'] = f'{fname_obj.GetSchemaName}.{ParentTable}'
    
    #stitch multiple parts
    part_map = {n:[k for k in ungroup_map.keys() if ungroup_map[k] == n] for n in set(ungroup_map.values())}
    for key, parts in part_map.items():
        #---simple benchmark-- start
        start = time.time() 

        #stitch part for one table
        try: 
            SfExec_StitchParts(conn.cursor(),key,parts)
        except:
            continue
        
        #---simple benchmark-- end
        end = time.time()
        delta = round(end - start)
        
        benchmk_data.append(["post-process",
                             f'stitch multi-part table:{key}',
                             f'{len(parts)} parts',
                             "",
                             delta])
    
        # stepwise report
        print(f'post-process: finish stitch {len(parts)} parts of file {key} in {delta} secs')
        
        #---write benchmark result to .csv
        np.savetxt(file_path, benchmk_data, delimiter = ',',fmt="%s")

def main():
