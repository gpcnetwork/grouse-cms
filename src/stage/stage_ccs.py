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

#ccs mapping type
# 1. icd9 to ccs mapping
# 2. icd10cm to ccs mapping
# 3. icd10pcs to ccs mapping
# 4. cpt to ccs mapping
ccs_mapping_type = 1

#if skip download
skip_download = False

#specify absolute paths
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
tmp_dir_path = f'{dir_path}/staging/tmp_dir'
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

# download and extract files in a temporary location
if not skip_download:
    if ccs_mapping_type == 1:
        resp = urlopen(config_data["ccs_keys"]["ccs_icd9_multiple_url"])
    elif ccs_mapping_type == 2:
        resp = urlopen(config_data["ccs_keys"]["ccs_icd10cm_url"])
    elif ccs_mapping_type == 3:
        resp = urlopen(config_data["ccs_keys"]["ccs_icd10pcs_url"])
    elif ccs_mapping_type == 4:
        resp = urlopen(config_data["ccs_keys"]["ccs_cpt_url"])
    else: 
        print("ccs mapping type not exist!")
    zipped_file = ZipFile(BytesIO(resp.read()))
    # unzip
    zipped_file.extractall(f'{dir_path}/staging/tmp_dir') # tmp_dir will be created under root folder
    file_lst = zipped_file.namelist()
    print(f'files downloaded:{file_lst} ')

# write useful csv files from temp folder to snowflake
user = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["user_secret"])
pwd = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["pwd_secret"])
acct = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["acct_secret"])
params = config_data["snowflake_csv_stg"]

snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    params = config_data["snowflake_csv_stg"]
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)
    
    upload_handler={}
    # 1. ICD9 to CCS
    if ccs_mapping_type == 1:
        upload_handler["icd9_ccs_dx"] = ["ICD9","CCSLVL1","CCSLVL1LABEL","CCSLVL2","CCSLVL2LABEL","CCSLVL3","CCSLVL3LABEL","CCSLVL4","CCSLVL4LABEL"] #hard-coded
        icd9_ccs_dx = pd.read_csv(f'{tmp_dir_path}/ccs_multi_dx_tool_2015.csv',header=None,skiprows = 1, names = upload_handler["icd9_ccs_dx"])
        
        upload_handler["icd9_ccs_px"] = ["ICD9","CCSLVL1","CCSLVL1LABEL","CCSLVL2","CCSLVL2LABEL","CCSLVL3","CCSLVL3LABEL"] #hard-coded
        icd9_ccs_px = pd.read_csv(f'{tmp_dir_path}/ccs_multi_pr_tool_2015.csv',header=None,skiprows = 1, names = upload_handler["icd9_ccs_px"])
        
        upload_handler["icd9_ccs_dx_ref"] = ["CCSLVL","CCSLVL_LABEL"] #hard-coded
        icd9_ccs_dx_ref = pd.read_csv(f'{tmp_dir_path}/dxmlabel-13.csv',header=None,skiprows = 1, names = upload_handler["icd9_ccs_dx_ref"])
        
        upload_handler["icd9_ccs_px_ref"] = ["CCSLVL","CCSLVL_LABEL"] #hard-coded
        icd9_ccs_px_ref = pd.read_csv(f'{tmp_dir_path}/prmlabel-09.csv',header=None,skiprows = 1, names = upload_handler["icd9_ccs_px_ref"])
        
    # 2. ICD10CM to CCS
    elif ccs_mapping_type == 2:
        ccs_vrsn = "v2022-1" #ccs version number

        upload_handler["icd10cm_ccsr"] = ["ICD10CM","ICD10CM_LABEL","CCSR_IP","CCSR_IP_LABEL","CCSR_OP","CCSR_OP_LABEL","CCSR1","CCSR1_LABEL","CCSR2","CCSR2_LABEL","CCSR3","CCSR3_LABEL","CCSR4","CCSR4_LABEL","CCSR5","CCSR5_LABEL","CCSR6","CCSR6_LABEL"] #hard-coded
        icd10cm_ccsr = pd.read_csv(f'{tmp_dir_path}/DXCCSR_{ccs_vrsn}.CSV',header=None,skiprows = 1,names=upload_handler["icd10cm_ccsr"])
        
        upload_handler["icd10cm_ccsr_ref"] = ["CCSR","CCSR_LABEL"] #hard-coded
        icd10cm_ccsr_ref = pd.read_excel(f'{tmp_dir_path}/DXCCSR-Reference-File-{ccs_vrsn}.xlsx',sheet_name = 2,header=None,skiprows = 2, names=upload_handler["icd10cm_ccsr_ref"])
        
        upload_handler["icd10cm_ccsr_ref_bodysys"] = ["BODY_SYS_LABEL","BODY_SYS_ABBR"] #hard-coded
        icd10cm_ccsr_ref_bodysys = pd.read_excel(f'{tmp_dir_path}/DXCCSR-Reference-File-{ccs_vrsn}.xlsx',sheet_name = 1,header=None,skiprows = 2, names=upload_handler["icd10cm_ccsr_ref_bodysys"])

    # 3. ICD10PCS to CCS
    elif ccs_mapping_type == 3:
        ccs_vrsn = "v2022-1" #ccs version number
        
        upload_handler["icd10pcs_ccsr"] = ["ICD10PCS","ICD10PCS_LABEL","CCSR","CCSR_LABEL","CLIN_DOMAIN"] #hard-coded
        icd10pcs_ccsr = pd.read_csv(f'{tmp_dir_path}/PRCCSR_{ccs_vrsn}.CSV',header=None,skiprows = 1, names = upload_handler["icd10pcs_ccsr"])
        
        upload_handler["icd10pcs_ccsr_ref"] = ["CCSR","CCSR_LABEL","CLIN_DOMAIN","ICD10PCS_ROOT","ICD10PCS_BODY_PARTS","ICD10PCS_DEVICES","ICD10PCS_APPROACHES"] #hard-coded
        icd10pcs_ccsr_ref = pd.read_excel(f'{tmp_dir_path}/PRCCSR-Reference-File-{ccs_vrsn}.xlsx',sheet_name = 2,header=None,skiprows = 2, names = upload_handler["icd10pcs_ccsr_ref"])
        
        upload_handler["icd10pcs_ccsr_ref_clindomain"] = ["CLIN_DOMAIN","CLIN_DOMAIN_ABBR"] #hard-coded
        icd10pcs_ccsr_ref_clindomain = pd.read_excel(f'{tmp_dir_path}/PRCCSR-Reference-File-{ccs_vrsn}.xlsx',sheet_name = 1,header=None,skiprows = 2, names = upload_handler["icd10pcs_ccsr_ref_clindomain"])
        
    # 4. CPT to CCS
    elif ccs_mapping_type == 4:
        ccs_vrsn = "v2021-1" #ccs version number
        
        upload_handler["cpt_ccs"] = ["CPT_RANGE","CCSLVL","CCSLVL_LABEL"] #hard-coded
        cpt_ccs = pd.read_csv(f'{tmp_dir_path}/CCS_services_procedures_{ccs_vrsn}.csv',header=None,skiprows = 2, names = upload_handler["cpt_ccs"])
        
        upload_handler["cpt_ccs_ref"] = ["CCSLVL","CCSLVL_LABEL"] #hard-coded
        cpt_ccs_ref = pd.read_excel(f'{tmp_dir_path}/CCS-SvcProc-Ref-File-{ccs_vrsn}.xlsx',sheet_name = 1,header=None,skiprows = 2, names = upload_handler["cpt_ccs_ref"])
        
    else: 
        print("ccs mapping type not exist!")
    
    # start uploading data
    for table_name, meta in upload_handler.items():
        sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],table_name.upper(),meta)
        conn.cursor().execute(sql_generator.GenerateDDL())

        # write dataframe to snowflake
        params["tgt_table"] = table_name.upper()
        load.SfWrite_PandaDF(conn,params,eval(table_name))

# remove temp folder after writing all csv files to snowflake
try:
    shutil.rmtree(tmp_dir_path)
except OSError as e:
    print ("Error: %s - %s." % (e.filename, e.strerror))

utils.pyclean()