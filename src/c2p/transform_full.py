#####################################################################     
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: transform_full.py                                                 
# The file read Snowflake credential from secret manager and establish
# database connection using python connector; and send DML script 
# over to snowflake to perform data transformation steps                                              
#####################################################################
# BEFORE YOU START, 
# a. Make sure the ../config.json file is correct
#####################################################################

import json
import os
import pandas as pd
from .. stage import load, stage_nppes
from snowflake.connector.pandas_tools import write_pandas

# load configuration file
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

#get snowflake connections strings from secretmanager
user = load.AWSSecrets(secret_name = config_data["aws"]["user_secret"])
pwd = load.AWSSecrets(secret_name = config_data["aws"]["pwd_secret"])
acct = load.AWSSecrets(secret_name = config_data["aws"]["acct_secret"])

# create snowflake connection context
# Note: disconnect automatically at the end of the "with" chunk
snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # Step 1 - create table shells
    for filename in os.listdir("./ddl"):
        if "_stg_" in filename:
            # ddl for staging tables
            load.SfExec_EnvSetup(conn.cursor(),config_data["snowflake_c2p_stg"])
            load.SfExec_ScriptsFromFile(conn.cursor(),f'./ddl/{filename}')
        else:
            # ddl for final cdm tables
            load.SfExec_EnvSetup(conn.cursor(),config_data["snowflake_c2p_main"])
            load.SfExec_ScriptsFromFile(conn.cursor(),f'./ddl/{filename}')

    # Step 2 - load reference tables
    load.SfExec_EnvSetup(conn.cursor(),config_data["snowflake_c2p_stg"])
    for filename in os.listdir("../ref"):
        cmap = pd.read_csv(f'../ref/{filename}', sep = ',')
        load.SfWrite_PandaDF(conn, cmap, filename.split(".")[0].upper())

    # Step 3 - Slice and Stage source CMS tables 
    # Step 4 - Perform trasnformation 
    for filename in os.listdir("./stored_procedures"):
        load.SfExec_ScriptsFromFile(conn.cursor(),f'./stored_procedures/{filename}')
    # define a wrapper function to perform both staging and transformation steps
    # could use different environment parameters (e.g. warehouse, schema)
    def stage_and_transform(cdm_tbl):
        load.SfExec_EnvSetup(conn.cursor(),config_data["snowflake_c2p_stg"])
        load.SfExec_ScriptsFromFile(conn.cursor(),f'./dml/{cdm_tbl}_stg_dml.sql')
        load.SfExec_EnvSetup(conn.cursor(),config_data["snowflake_c2p_main"])
        load.SfExec_ScriptsFromFile(conn.cursor(),f'./dml/{cdm_tbl}_dml.sql',config_data["snowflake_c2p_stg"]["env_schema"])
    # don't change the order, as there are some dependencies
    stage_and_transform("enrollment")
    stage_and_transform("demographic")
    stage_and_transform("death")
    stage_and_transform("lds_address_history")
    stage_and_transform("encounter")
    stage_and_transform("diagnosis")
    stage_and_transform("procedures")
    stage_and_transform("dispensing")
    stage_and_transform("provider")