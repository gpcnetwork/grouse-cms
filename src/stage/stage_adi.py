'''
Wiscosin Area Deprivation Index (with Zip9 to Census Block Group mapping) 
> https://www.neighborhoodatlas.medicine.wisc.edu/

Manual data downloads are required: 
1. go to https://www.neighborhoodatlas.medicine.wisc.edu/download
2. register an account and log in for data download
3. download the ADI rankings for each state (choose 2019 version) and rename file
- select "9-digit ZIP codes" as linkage format and "Single State"
- default name of download seems to always be "adi-download" (but may change over time)
- standardize the file name to "adi-download-<year of file>-<state>.zip" for easy retrivial 
- assign the most frequent CBG to zip5+0000 values

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
    adi_zfs = [f for f in s3obj[''] if 'adi-download-2020' in f]
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
        fn = [f for f in os.listdir(download_dir) if '.csv' in f][0]
        df = pd.read_csv(f'{download_dir}/{fn}',
                         usecols=['ZIP_4','FIPS','ADI_NATRANK','ADI_STATERANK'],
                         dtype={'ZIP_4':'str','FIPS':'str','ADI_NATRANK':'str','ADI_STATERANK':'str'}).dropna()

        # maximum likelihood-based assignment of CBG for zip5+0000 values
        df2 = df[['ZIP_4','FIPS']].assign(ZIP5 = lambda df:df.ZIP_4.str[:5]+'0000')
        dfzip5 = pd.merge(df2.groupby(['ZIP5'])['FIPS'].agg(lambda x: pd.Series.mode(x)[0]).to_frame().reset_index(),df,how="inner",on="FIPS")
        dfzip5 = dfzip5.drop(columns="ZIP_4").rename(columns = {'ZIP5':'ZIP_4'}).drop_duplicates()
        df3 = pd.concat([df,dfzip5])
        
        # split table into:  
        #  a) zip <-> BG mapping; and
        params["env_schema"] = "GEOID_MAPPING"
        params["tgt_table"] = "Z9_TO_BG"
        df = df3[['ZIP_4','FIPS']].rename(columns = {'ZIP_4':'GEOID_FROM', 'FIPS':'GEOID_TO'}).drop_duplicates()
        if idx == 0:
            sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],list(df.columns))
            conn.cursor().execute(sql_generator.GenerateDDL())
            print(f'{params["env_schema"]}.{params["tgt_table"]} was refreshed!')
        load.SfWrite_PandaDF(conn,params,df)
        
        #  b) ADI table
        params["env_schema"] = config_data["sdoh_keys"]["sf_env_schema"]
        params["tgt_table"] = "ADI_BG"
        df =  df3[['FIPS','ADI_NATRANK','ADI_STATERANK']].rename(columns = {'FIPS':'FIPS_BG'}).drop_duplicates()
        if idx == 0:
            sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],list(df.columns))
            conn.cursor().execute(sql_generator.GenerateDDL())
            print(f'{params["env_schema"]}.{params["tgt_table"]} was refreshed!')
        load.SfWrite_PandaDF(conn,params,df)
       
        # remove temp download folder after writing all csv files to snowflake
        try:
            shutil.rmtree(download_dir)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))

# garbage clean
utils.pyclean()
