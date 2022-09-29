'''
AHRQ HCUP CCS mappings
'''
import os
import json
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import utils
import load
import extract
import pandas as pd
import shutil
# or: requests.get(url).content

#phecode mapping type
# 1. icd9cm to phecode v1.2
# 2. icd10am to phecode v1.2
# 3. phecode v1.2 definition
# 4. icd9cm to phecode x
# 5. icd10cm to phecode x 
# 6. phecode x definition
phecode_mapping_type = 6

#if skip download
skip_download = False

#specify absolute paths
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
download_dir = os.path.dirname(dir_path)+'/tmp_dir'
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

# download and extract files in a temporary location
if not skip_download:
    if phecode_mapping_type == 1:
        resp = urlopen(config_data["phecode_keys"]["phecodev1dot2_icd9cm_url"])
    elif phecode_mapping_type == 2:
        resp = urlopen(config_data["phecode_keys"]["phecodev1dot2_icd10cm_url"])
    elif phecode_mapping_type == 3:
        resp = urlopen(config_data["phecode_keys"]["phecode_def_1dot2"])
    elif phecode_mapping_type == 4:
        resp = urlopen(config_data["phecode_keys"]["phecodex_icd9cm_url"])
    elif phecode_mapping_type == 5:
        resp = urlopen(config_data["phecode_keys"]["phecodex_icd10cm_url"])
    elif phecode_mapping_type == 6:
        resp = urlopen(config_data["phecode_keys"]["phecode_def_x"])
    else: 
        print("phecode mapping type not exist!")
    
    # create tmp folder
    if not os.path.isdir(download_dir):
        os.makedirs(download_dir)
    
    # unzip
    zipped_file = ZipFile(BytesIO(resp.read()))
    zipped_file.extractall(download_dir)
    file_lst = zipped_file.namelist()
    print(f'files downloaded:{file_lst} ')

# write useful csv files from temp folder to snowflake
sf_params_on_aws = config_data["aws_grouse_default"]
user = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["user_secret"])
pwd = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["pwd_secret"])
acct = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["acct_secret"])
params = config_data["snowflake_cms_admin_default"]

snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)
    
    upload_handler={}
    # 1. icd9cm to phecode v1.2
    if phecode_mapping_type == 1:
        icd9cm_phecode = pd.read_csv(f'{download_dir}/phecode_icd9_map_unrolled.csv',header = 0,skiprows = 0)
        upload_handler["icd9cm_phecode"] = list(icd9cm_phecode.columns)
    
    # 2. icd10am to phecode v1.2
    elif phecode_mapping_type == 2:
        icd10cm_phecode = pd.read_csv(f'{download_dir}/Phecode_map_v1_2_icd10cm_beta.csv',header = 0,skiprows = 0, encoding='latin1')
        upload_handler["icd10cm_phecode"] = list(icd10cm_phecode.columns)
        
    # 3. phecode v1.2 definition    
    elif phecode_mapping_type == 3:
        phecode_ref = pd.read_csv(f'{download_dir}/phecode_definitions1.2.csv',header = 0,skiprows = 0)
        upload_handler["phecode_ref"] = list(phecode_ref.columns)
 
    # 4. icd9cm to phecode x
    elif phecode_mapping_type == 4:
        icd9cm_phecodex = pd.read_csv(f'{download_dir}/ICD9_to_phecode_V2.csv',header = 0,skiprows = 0)
        upload_handler["icd9cm_phecodex"] = list(icd9cm_phecodex.columns)
  
    # 5. icd10cm to phecode x
    elif phecode_mapping_type == 5:
        icd10cm_phecodex = pd.read_csv(f'{download_dir}/ICD10_to_phecode_V2.csv',header = 0,skiprows = 0)
        upload_handler["icd10cm_phecodex"] = list(icd10cm_phecodex.columns)
    
    # 6. phecode x definition
    elif phecode_mapping_type == 6:
        phecodex_ref = pd.read_csv(f'{download_dir}/phecode_strings_V2.csv',header = 0,skiprows = 0, encoding='latin1')
        upload_handler["phecodex_ref"] = list(phecodex_ref.columns)
        
    else: 
        print("phecode mapping type not exist!")
    
    # start uploading data
    for table_name, meta in upload_handler.items():
        sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],table_name.upper(),meta)
        conn.cursor().execute(sql_generator.GenerateDDL())

        # write dataframe to snowflake
        params["tgt_table"] = table_name.upper()
        load.SfWrite_PandaDF(conn,params,eval(table_name))

# remove temp folder after writing all csv files to snowflake
try:
    shutil.rmtree(download_dir)
except OSError as e:
    print ("Error: %s - %s." % (e.filename, e.strerror))

utils.pyclean()
