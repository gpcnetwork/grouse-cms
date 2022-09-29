'''
ACS derived variables download and staging
> https://raw.githubusercontent.com/UNMC-CRANE/SDH/main/SDH_Stats.csv
'''
import os
import json
from urllib.request import urlopen
import utils
import load
import extract
import pandas as pd
import shutil
# or: requests.get(url).content

# specify absolute paths
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
download_dir = os.path.dirname(dir_path)+'/tmp_dir'
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

# load and clean
df = pd.read_csv(config_data["sdoh_keys"]["acs_url"],header = 0)
df.rename(columns={"GEOGRAPHY_ID": "FIPS_CT"}, inplace=True)

# write useful csv files from temp folder to snowflake
sf_params_on_aws = config_data["aws_grouse_default"]
user = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["user_secret"])
pwd = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["pwd_secret"])
acct = load.AWSSecrets(region_name=sf_params_on_aws["region"], secret_name = sf_params_on_aws["acct_secret"])
params = config_data["snowflake_cms_admin_default"]

snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # set up the snowflake environment
    params["env_schema"] = config_data["sdoh_keys"]["sf_env_schema"]
    params["tgt_table"] = config_data["sdoh_keys"]["acs_stg_table"]
    load.SfExec_EnvSetup(conn.cursor(),params)
    
    # start upload data
    sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],list(df.columns))
    conn.cursor().execute(sql_generator.GenerateDDL())

    # write dataframe to snowflake
    load.SfWrite_PandaDF(conn,params,df)

utils.pyclean()
