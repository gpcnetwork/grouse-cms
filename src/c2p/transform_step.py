#####################################################################     
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: transform_step.py                                                 
# The file read Snowflake credential from secret manager and establish
# database connection using python connector; and send DML script 
# over to snowflake to perform data transformation steps                                              
#####################################################################
# BEFORE YOU START, 
# a. Make sure the ../config.json file is correct
# b. You can comment out chunk of codes of each step to run 
#    part of the code at a time. Please do so according to the step orders
#####################################################################
import sys
import json
import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# load custom module
sys.path.append(os.path.abspath(f'{os.path.dirname(os.path.dirname(__file__))}/stage'))
import load

# load configuration file
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
download_dir = os.path.dirname(dir_path)+'/tmp_dir'
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

#get snowflake connections strings from secretmanager
sf_params_on_aws = config_data["aws_grouse_default"]
user = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["user_secret"])
pwd = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["pwd_secret"])
acct = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["acct_secret"])
snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
params = config_data["snowflake_cms_admin_default"]
params["env_schema"] = 'DEV'
print(params)

# Step 1 - create table shells
def create_tbl_shell(snowflake_conn,params)->None:
    with snowflake_conn as conn:
        # execute DDL scripts
        for filename in os.listdir(f'{dir_path}/c2p/ddl'):
            if "_stg_" in filename:
                # ddl for staging tables
                load.SfExec_EnvSetup(conn.cursor(),params)
                load.SfExec_ScriptsFromFile(conn.cursor(),f'{dir_path}/c2p/ddl/{filename}')
            else:
                # ddl for final cdm tables
                params["env_schema"] = config_data["cms_keys"]["sf_c2p_schema"]
                load.SfExec_EnvSetup(conn.cursor(),params)
                load.SfExec_ScriptsFromFile(conn.cursor(),f'{dir_path}/c2p/ddl/{filename}')
            print("Table shell created: ", filename)

# Step 2 - load reference tables
def load_ref_tbl(snowflake_conn,params)->None:
    with snowflake_conn as conn:
        # set up the snowflake environment
        load.SfExec_EnvSetup(conn.cursor(),params)
        dir_path2 = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        for filename in os.listdir(f'{dir_path2}/ref'):
            if os.path.isfile(f'{dir_path2}/ref/{filename}'):
                cmap = pd.read_csv(f'{dir_path2}/ref/{filename}', sep = ',')
                load.SfWrite_PandaDF(conn, cmap, filename.split(".")[0].upper())
            else:
                continue
            print("Reference table loaded: ", filename)

# Step 3 - Slice and Stage source CMS tables 
# Step 4 - Perform trasnformation 
def c2p_transform_main(snowflake_conn,params)->None:
    with snowflake_conn as conn:
        # set up the snowflake environment
        load.SfExec_EnvSetup(conn.cursor(),params)
        # execute stored procedures
        for filename in os.listdir(f'{dir_path}/c2p/stored_procedures'):
            load.SfExec_ScriptsFromFile(conn.cursor(),f'{dir_path}/c2p/stored_procedures/{filename}')
        # define a wrapper function to perform both staging and transformation steps
        # could use different environment parameters (e.g. warehouse, schema)
        def stage_and_transform(cdm_tbl):
            params = config_data["snowflake_cms_admin_default"]
            load.SfExec_EnvSetup(conn.cursor(),params)
            load.SfExec_ScriptsFromFile(conn.cursor(),f'{dir_path}/c2p/dml/{cdm_tbl}_stg_dml.sql')
            params["env_schema"] = config_data["cms_keys"]["sf_c2p_schema"]
            load.SfExec_EnvSetup(conn.cursor(),params)
            load.SfExec_ScriptsFromFile(conn.cursor(),f'{dir_path}/c2p/dml/{cdm_tbl}_dml.sql')
            print("CDM table transformed: ", cdm_tbl)
            
        # don't change the order, as there are some dependencies
        # ---- patient-level
        stage_and_transform("enrollment")
        stage_and_transform("demographic")
        stage_and_transform("death")
        stage_and_transform("address_history")
        stage_and_transform("address_geocode") # depend on address_history
        stage_and_transform("obs_comm") # depend on address_history
        # ---- encounter-level
        stage_and_transform("encounter")
        stage_and_transform("diagnosis") # depend on encounter
        stage_and_transform("procedures") # depend on encounter
        stage_and_transform("dispensing") # depend on encounter
        # ---- provider-level
        stage_and_transform("provider")  # depend on encounter


if __name__ == "__main__":
    create_tbl_shell(snowflake_conn,params)
    
    # load_ref_tbl(snowflake_conn,params)
    
    # params["env_wh"] = config_data["cms_keys"]["sf_med_wh"]
    # c2p_transform_main(snowflake_conn,params)