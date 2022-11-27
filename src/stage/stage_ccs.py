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
# 2. icd10cm to ccsr mapping
# 3. icd10pcs to ccs mapping
# 4. cpt to ccs mapping
# 5. icd10cm to ccs mapping
ccs_mapping_type = 4

#if skip download
skip_download = False

#specify absolute paths
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
tmp_dir_path = f'{dir_path}/staging/tmp_dir'
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

# download and extract files in a temporary location
resp = list()
ccs_vrsn = list()
if not skip_download:
    if ccs_mapping_type == 1:
        resp.append(urlopen(config_data["ccs_keys"]["ccs_icd9_multiple_url"]))
        resp.append(urlopen(config_data["ccs_keys"]["ccs_icd9_single_url"]))
        
    elif ccs_mapping_type == 2:
        for item in config_data["ccs_keys"]["ccsr_icd10cm_urls"]:
            ccs_vrsn.extend(list(item.keys()))
            resp.append(urlopen(item[list(item.keys())[0]]))
            
    elif ccs_mapping_type == 3:
        for item in config_data["ccs_keys"]["ccs_icd10pcs_urls"]:
            ccs_vrsn.extend(list(item.keys()))
            resp.append(urlopen(item[list(item.keys())[0]]))
            
    elif ccs_mapping_type == 4:
        for item in config_data["ccs_keys"]["ccs_cpt_urls"]:
            ccs_vrsn.extend(list(item.keys()))
            resp.append(urlopen(item[list(item.keys())[0]]))
            
    elif ccs_mapping_type == 5:
        for item in config_data["ccs_keys"]["ccs_icd10cm_urls"]:
            ccs_vrsn.extend(list(item.keys()))
            resp.append(urlopen(item[list(item.keys())[0]]))
            
    else: 
        print("ccs mapping type not exist!")
    
    csv_name = list()
    ref_name = list()
    for resp_file in resp:
        zipped_file = ZipFile(BytesIO(resp_file.read())) # unzip
        zipped_file.extractall(f'{dir_path}/staging/tmp_dir') # tmp_dir will be created under root folder
        file_lst = zipped_file.namelist()
        csv_name.extend([i for i in file_lst if ".csv" in i])
        ref_name.append([i for i in file_lst if "ref-file" in i.lower()])
        print(f'files downloaded:{file_lst} ')

# write useful csv files from temp folder to snowflake
sf_params_on_aws = config_data["aws_grouse_default"]
user = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["user_secret"])
pwd = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["pwd_secret"])
acct = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["acct_secret"])
params = config_data["snowflake_cms_admin_default"]
params["env_schema"] = "GROUPER_VALUESETS"

snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)
    
    upload_handler={}
    # 1. ICD9 to CCS
    if ccs_mapping_type == 1:
        # ICD9 DX
        upload_handler["icd9dx_ccs"] = ["ICD9","CCS_SLVL1","CCS_SLVL1LABEL","ICD9_LABEL","CCS_MLVL1","CCS_MLVL1LABEL","CCS_MLVL2","CCS_MLVL2LABEL","CCS_MLVL3","CCS_MLVL3LABEL","CCS_MLVL4","CCS_MLVL4LABEL"] #hard-coded
        ccsm_names = upload_handler["icd9dx_ccs"][0:1] + upload_handler["icd9dx_ccs"][4:]
        ccss_names = upload_handler["icd9dx_ccs"][0:4]
        icd9dx_ccsm = pd.read_csv(f'{tmp_dir_path}/ccs_multi_dx_tool_2015.csv',header=None,skiprows = 1, names = ccsm_names,index_col=False)
        icd9dx_ccss = pd.read_csv(f'{tmp_dir_path}/$dxref 2015.csv',header=None,skiprows = 3, names = ccss_names,index_col=False).iloc[:,0:3]
        icd9dx_ccs = icd9dx_ccss.join(icd9dx_ccsm.set_index("ICD9"),on="ICD9")
        
        upload_handler["icd9dx_ccs_ref"] = ["CCSLVL","CCSLVL_LABEL","CCSLVL_TYPE"] #hard-coded
        icd9dx_ccsm_ref = pd.read_csv(f'{tmp_dir_path}/dxmlabel-13.csv',header=None,skiprows = 1, names = upload_handler["icd9dx_ccs_ref"][0:2],index_col=False)
        icd9dx_ccsm_ref["CCSLVL_TYPE"] = "M"
        icd9dx_ccss_ref = pd.read_csv(f'{tmp_dir_path}/dxlabel 2015.csv',header=None,skiprows = 1, names = upload_handler["icd9dx_ccs_ref"][0:2],index_col=False)
        icd9dx_ccss_ref["CCSLVL_TYPE"] = "S"
        icd9dx_ccss["ICD9"].str.strip() # clean up trailing space
        icd9dx_ccss["CCS_SLVL1"].str.strip() # clean up trailing space
        icd9dx_ccs_ref = pd.concat([icd9dx_ccsm_ref,icd9dx_ccss_ref], ignore_index=True, sort=False)
        
        # ICD9 PX
        upload_handler["icd9px_ccs"] = ["ICD9","CCS_SLVL1","CCS_SLVL1LABEL","ICD9_LABEL","CCS_MLVL1","CCS_MLVL1LABEL","CCS_MLVL2","CCS_MLVL2LABEL","CCS_MLVL3","CCS_MLVL3LABEL"] #hard-coded
        ccsm_names = upload_handler["icd9px_ccs"][0:1] + upload_handler["icd9px_ccs"][4:]
        ccss_names = upload_handler["icd9px_ccs"][0:4]
        icd9px_ccsm = pd.read_csv(f'{tmp_dir_path}/ccs_multi_pr_tool_2015.csv',header=None,skiprows = 1, names = ccsm_names,index_col=False)
        icd9px_ccss = pd.read_csv(f'{tmp_dir_path}/$prref 2015.csv',header=None,skiprows = 3, names = ccss_names,index_col=False).iloc[:,0:3]
        icd9px_ccss["ICD9"].str.strip() # clean up trailing space
        icd9px_ccss["CCS_SLVL1"].str.strip() # clean up trailing space
        icd9px_ccs = icd9px_ccss.join(icd9px_ccsm.set_index("ICD9"),on="ICD9")
        
        upload_handler["icd9px_ccs_ref"] = ["CCSLVL","CCSLVL_LABEL","CCSLVL_TYPE"] #hard-coded
        icd9px_ccsm_ref = pd.read_csv(f'{tmp_dir_path}/prmlabel-09.csv',header=None,skiprows = 1, names = upload_handler["icd9px_ccs_ref"][0:2],index_col=False)
        icd9px_ccsm_ref["CCSLVL_TYPE"] = "M"
        icd9px_ccss_ref = pd.read_csv(f'{tmp_dir_path}/prlabel 2014.csv',header=None,skiprows = 1, names = upload_handler["icd9px_ccs_ref"][0:2],index_col=False)
        icd9px_ccss_ref["CCSLVL_TYPE"] = "S"
        icd9px_ccs_ref = pd.concat([icd9px_ccsm_ref,icd9px_ccss_ref], ignore_index=True, sort=False)
        
    # 2. ICD10CM to CCSR
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
        upload_handler["cpt_ccs"] = ["CPT_RANGE","CCSLVL","CCSLVL_LABEL","CPT_LB","CPT_UB","VRSN"] #hard-coded
        upload_handler["cpt_ccs_ref"] = ["CCSLVL","CCSLVL_LABEL","VRSN"] #hard-coded
        cpt_ccs = list()
        cpt_ccs_ref = list()
        for idx, vrsn in enumerate(ccs_vrsn):
            # cpt to ccs mapping
            cpt_ccs_idx = pd.read_csv(f'{tmp_dir_path}/{csv_name[idx]}',header=None,skiprows = 2, names = upload_handler["cpt_ccs"])
            cpt_ccs_idx[['CPT_LB', 'CPT_UB']] = cpt_ccs_idx['CPT_RANGE'].str.replace("'","").str.split('-', 1, expand=True)
            cpt_ccs_idx["VRSN"] = vrsn
            cpt_ccs.append(cpt_ccs_idx)
            # ccs reference
            if ref_name[idx]:
                cpt_ccs_ref_idx = pd.read_excel(f'{tmp_dir_path}/{ref_name[idx][0]}',sheet_name = 1,header=None,skiprows = 2, names = upload_handler["cpt_ccs_ref"])
                cpt_ccs_ref_idx["VRSN"] = vrsn
                cpt_ccs_ref.append(cpt_ccs_ref_idx)
        cpt_ccs = pd.concat(cpt_ccs).drop_duplicates()
        cpt_ccs_ref = pd.concat(cpt_ccs_ref).drop_duplicates()
        
    # 5. ICD10CM to CCS
    elif ccs_mapping_type == 5:
        ccs_vrsn = "2019_1" #ccs version number
        
        upload_handler["icd10cm_ccs"] = ["ICD10CM","CCS_SLVL1","ICD10CM_LABEL","CCS_SLVL1LABEL","CCS_MLVL1","CCS_MLVL1LABEL","CCS_MLVL2","CCS_MLVL2LABEL"] #hard-coded
        icd10cm_ccs = pd.read_csv(f'{tmp_dir_path}/ccs_dx_icd10cm_{ccs_vrsn}.csv',header=None,skiprows = 1, names = upload_handler["icd10cm_ccs"])
            
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