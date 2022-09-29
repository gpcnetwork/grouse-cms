'''
Wiscosin Area Deprivation Index (with Zip9 to Census Block Group mapping) 
> https://www.neighborhoodatlas.medicine.wisc.edu/

Manual data downloads are required: 
1. go to https://www.neighborhoodatlas.medicine.wisc.edu/download
2. register an account and log in for data download
3. download the ADI rankings for each state (choose 2019 version) and rename file
- default name of download seems to always be "adi-download" (but may change over time)
- standardize the file name to "adi-download-2019-<state>.zip" for easy retrivial 

@params PATH_TO_ADI_DIR = path to the download folder of ADI zipped files
'''
import os
import json
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import load 
import extract
import utils
import pandas as pd
import shutil

skip_download = False
skip_unzip = False

# parameters
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
download_dir = os.path.dirname(dir_path)+'/tmp_dir'
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))
adi_bucket = config_data["sdoh_keys"]["s3_bucket_source"]

# collect adi zipped file names from source bucket
try:
    s3obj = utils.get_objects(adi_bucket)
    adi_zfs = [f for f in s3obj[''] if 'adi-download' in f]
except Exception as e:
    print(f'Error:{e}')

# setup snowflake connection
sf_params_on_aws = config_data["aws_grouse_default"]
user = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["user_secret"])
pwd = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["pwd_secret"])
acct = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["acct_secret"])
snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # set up the snowflake environment
    params = config_data["snowflake_cms_admin_default"]
    params["env_schema"] = config_data["sdoh_keys"]["sf_env_schema"]
    params["tgt_table"] = config_data["sdoh_keys"]["adi_stg_table"]
    load.SfExec_EnvSetup(conn.cursor(),params)
        
    # download, unzip, process and integrate
    for idx, zf in enumerate(adi_zfs):
        # create tmp folder
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)
            
        # download
        if not skip_download:
            load.Download_S3Objects(adi_bucket,zf,f'{download_dir}/{zf}')
            
        # unzip
        if not skip_unzip:
            zipped_file = ZipFile(f'{download_dir}/{zf}')
            zipped_file.extractall(download_dir)
            file_lst = zipped_file.namelist()
            print(f'files unzipped:{file_lst} ')
        
        # read and clean
        fn = [f for f in os.listdir(download_dir) if '.txt' in f and not 'ReadMe' in f][0]
        df = pd.read_csv(f'{download_dir}/{fn}',
                         usecols=['ZIPID','FIPS.x','ADI_NATRANK','ADI_STATERNK'],
                         dtype={'ZIPID':'str','FIPS.x':'str','ADI_NATRANK':'str','ADI_STATERNK':'str'})
        df.rename(columns={"FIPS.x": "GEOID_CBG"}, inplace=True)
        
        # start upload to snowflake
        if idx == 1:
            sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],list(df.columns))
            conn.cursor().execute(sql_generator.GenerateDDL())
        load.SfWrite_PandaDF(conn,params,df)

        # remove temp download folder after writing all csv files to snowflake
        try:
            shutil.rmtree(download_dir)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))
    
# garbage clean
utils.pyclean()
