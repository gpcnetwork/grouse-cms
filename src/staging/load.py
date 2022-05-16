#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: load.py                                                 
# The file read Snowflake credential from secret manager and establish
# database connection using python connector; and send DML script 
# over to snowflake to perform data staging steps                                              
#####################################################################
# BEFORE YOU START, 
# a. INSTALL DEPENDENCIES BY RUNNING ./dep/setup.sh 
# b. MAKE SURE extract.py IS LOADED UNDER THE SAME DIRECTORY
#####################################################################
import boto3
import ast
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from dataclasses import dataclass
from typing import Optional
from smart_open import open as s3open
import time
import numpy as np
from random import randint
import pandas as pd
from sas7bdat import SAS7BDAT
import pyreadstat
import s3fs
from datetime import datetime,date,timedelta
from os import remove


@dataclass
class AWSSecrets:
    secret_name: Optional[str] = None

    def GetSecretVal(self):
        """read connection strings= values from secret manager"""
        secrete_client = boto3.client(service_name='secretsmanager',region_name="us-east-2") #! region name is hard-coded
        return ast.literal_eval(secrete_client.get_secret_value(SecretId=self.secret_name)['SecretString'])[self.secret_name]

@dataclass
class SnowflakeParams:
    env_role: Optional[str] = None
    env_wh: Optional[str] = None
    env_db: Optional[str] = None
    stg_stage: Optional[str] = None
    stg_table: Optional[str] = None
    tgt_schema: Optional[str] = None
    tgt_table: Optional[str] = None

class SnowflakeConnection(object): 
    """
    initialize a connection context with snowflake database and safely close it when the task is done
    ref: https://medium.com/opex-analytics/database-connections-in-python-extensible-reusable-and-secure-56ebcf9c67fe
    """
    sfusr: AWSSecrets
    sfpwd: AWSSecrets
    sfacct: AWSSecrets
    
    def __init__(self,sfuser,sfpwd,sfacct):
        self.sfuser = sfuser
        self.sfpwd = sfpwd
        self.sfacct = sfacct
        self.connector = None

    def __enter__(self):
        # make connection
        self.connector = sf.connect(
            user = self.sfuser.GetSecretVal(),
            password = self.sfpwd.GetSecretVal(),
            account = self.sfacct.GetSecretVal()
        )
        return self.connector  # need to return connection object for "write_pandas" to work

    def __exit__(self,type,value,traceback):
        if traceback is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()

def SfExec_EnvSetup(conn,params:SnowflakeParams):
    """setup snowflake environment for staging tasks"""
    conn.execute(f'USE ROLE {params.env_role}')
    conn.execute(f'USE WAREHOUSE {params.env_wh}')
    conn.execute(f'USE DATABASE {params.env_db}')
    conn.execute(f'USE SCHEMA STAGING')
    conn.execute('ALTER SESSION SET DATE_INPUT_FORMAT = \'YYYYMMDD\'')
        
def SfExec_CreateFixedWidthTable(conn,params:SnowflakeParams):       
    """create table shell for input fixed-width data"""
    conn.execute(f'CREATE OR REPLACE TABLE {params.stg_table} \n'
                 f' (PLAIN_TEXT_COL varchar(4000))')

def SfExec_CopyIntoDat(conn,params:SnowflakeParams,fname):
    """copy .dat file into table shell"""
    conn.execute(f'TRUNCATE {params.stg_table}')
    conn.execute(f'COPY INTO {params.stg_table} \n'
                 f' FROM @{params.stg_stage}/dat_files/{fname}.dat \n'
                 f' ON_ERROR = \'abort_statement\'')

def SfExec_StitchParts(conn,tbl:str,parts:list,drop_after_merge=False):
    """for large cms, cdm tables, data is loaded in parts, which needed to be stitched together for easy query"""
    sql_create = '''CREATE OR REPLACE TABLE %(table_name)s AS 
                    %(union_statements)s;''' % dict (
                 table_name = tbl,
                 union_statements = '\n UNION \n'.join('SELECT * FROM %s' % part
                                                        for part in parts)
                )
    conn.execute(sql_create)
    
    if drop_after_merge: 
        # drop parts
        for part in parts:
            conn.execute(f'DROP TABLE IF EXISTS {part}')

# https://stackoverflow.com/questions/19472922/reading-external-sql-script-in-python
def SfExec_ScriptsFromFile(conn, path_to_file, args):
    # Open and read the file as a single buffer
    fd = open(path_to_file, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        try:
            conn.execute(command, args)
        except Exception as e:
            print("Command skipped: ", e)

def Download_SAS7bDAT(bucket_name,path_to_sas,src_sas,verbose=True)->None:
    # download data to local storage
    s3_client = boto3.resource('s3')
    s3_bucket = s3_client.Bucket(bucket_name)
    s3_bucket.download_file(Key = f'{src_sas}',
                            Filename = f'{path_to_sas}')
    if verbose: 
        print(f'file {src_sas} downloaded!')

def Read_SAS7bDAT(src_sas,row_offset=0,row_limit=-1,num_processes=1,verbose=True,encoding='utf-8')->list:
    # load into current session as pandas dataframe
    # https://github.com/pandas-dev/pandas/issues/13939#issuecomment-238378324
    # https://communities.sas.com/t5/SAS-Communities-Library/Accessing-AWS-S3-as-NFS-from-CAS-and-SAS-Part-1/ta-p/553373
    # https://github.com/Missouri-BMI/GROUSE/issues/35
    # sasdf = pd.read_sas(f'{src_sas}.sas7bdat')
    # sasdf = SAS7BDAT(f'{src_sas}.sas7bdat').to_data_frame()
    
    if num_processes > 1:
        # https://github.com/Roche/pyreadstat#reading-files-in-parallel-processes
        # https://stackoverflow.com/questions/63712214/pd-read-sav-and-pyreadstat-are-so-slow-how-can-i-speed-up-pandas-for-big-data-i
        if row_limit == -1:
            #load all rows
            #pyreadstat.read_file_multiprocessing doesn't seem to work... 
            sasdf, sasmeta = pyreadstat.read_file_multiprocessing(pyreadstat.read_sas7bdat,f'{src_sas}.sas7bdat', 
                                                                  num_processes=num_processes,
                                                                  disable_datetime_conversion=True,encoding=encoding)
        else:
            #load by chunks
            sasdf, sasmeta = pyreadstat.read_file_multiprocessing(pyreadstat.read_sas7bdat,f'{src_sas}.sas7bdat', 
                                                                  num_processes=num_processes,row_offset=row_offset,row_limit=row_limit,
                                                                  disable_datetime_conversion=True,encoding=encoding)
    else: 
        # no parallelization
        # https://ofajardo.github.io/pyreadstat_documentation/_build/html/index.html
        if row_limit == -1:
            #load all rows
            sasdf, sasmeta = pyreadstat.read_sas7bdat(f'{src_sas}.sas7bdat',
                                                      disable_datetime_conversion=True,encoding=encoding)
        else:
            #load by chunks
            sasdf, sasmeta = pyreadstat.read_sas7bdat(f'{src_sas}.sas7bdat',
                                                      row_offset=row_offset, row_limit=row_limit,
                                                      disable_datetime_conversion=True,encoding=encoding)
    
    if verbose: 
        print(f'file of size {sasdf.shape} read in memory!')
    
                                                               
    if sasdf.shape[0] == 0: #table can be empty
        print(f'Table {src_sas.upper()} has row counts: {sasdf.shape[0]}!') 
        return [False,[],sasmeta.original_variable_types]
    else: 
        # snowflake has a panda.tools library with seemingly a convenience function to directly write pandas dataframe to snowflake
        # however, there may be issues with date type compatibility
        def fix_date_cols(df, meta, tz = 'UTC'): 
            cols = [col for col in df if col.lower().endswith('_date')]
            for col in cols:
                # some date columns may auto-convert to datetime64 formate
                if 'DATETIME' in meta.original_variable_types[col].upper():
                    df[col] = [x.date() for x in df[col].dt.tz_localize(tz)]
                # some date columns may be converted to a 5-digit numbers representing numbers of days since 01/01/1960, SAS origin date
                elif any(x in meta.original_variable_types[col].upper() for x in ['MMDDYY','DATE']):
                    df[col] = [date(1960,1,1) + timedelta(days=x) for x in df[col].fillna(999999)] #999999 convert missing dates to a future date 4697-11-26
                # any other format remains the same until conversion error pops out when writing to snowflake
                else:
                    df[col] = df[col]
                # https://github.com/wesm/feather/issues/349
                # https://stackoverflow.com/questions/32888124/pandas-out-of-bounds-nanosecond-timestamp-after-offset-rollforward-plus-adding-a
                df[col] = pd.to_datetime(df[col],errors = 'coerce').dt.date #errored out any future dates (including the articial one)
            return(df)
        # fix all the date columns before writing to snowflake
        sasdf = fix_date_cols(sasdf, sasmeta)
        
        # convert column names into upper cases
        colnm_lower = sasdf.columns
        sasdf.columns = [x.upper() for x in colnm_lower]
        
        # next_row = False if full-load or last chunk 
        if row_limit==-1 or sasdf.shape[0]<row_limit:
            return [False,sasdf,sasmeta.original_variable_types]
        else: 
            return [True,sasdf,sasmeta.original_variable_types]
    
def SfWrite_PandaDF(conn,params:SnowflakeParams,sasdf,verbose=True):
        # write pandas dataframe to snowflake
        try:
            write_pandas(conn,sasdf,params.tgt_table,
                         database=params.env_db,schema=params.tgt_schema)
        except Exception as e:
            print(e)
            
        if verbose:
            print(f'{params.tgt_table}{sasdf.shape} written in snowflake')

