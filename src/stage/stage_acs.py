'''
ACS derived variables download and staging
> https://raw.githubusercontent.com/UNMC-CRANE/SDH/main/SDH_Stats.csv
> 
> https://raw.githubusercontent.com/UNMC-CRANE/SDH/main/SDH_Columns.csv 
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

# load data and clean
df_vs = pd.read_csv(config_data["sdoh_keys"]["acs_vs_url"],header = 0)
df_vs.rename(columns={"GEOGRAPHY_ID": "FIPS_CT"}, inplace=True)

# load metadata
df_col = pd.read_csv(config_data["sdoh_keys"]["acs_col_url"])
df_col['UNIT'] = [s[s.find("[")+1:s.find("]")].lower() for s in df_col['DESCRIPTION']]

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
    load.SfExec_EnvSetup(conn.cursor(),params)
    
    # upload acs values data
    params["tgt_table"] = "ACS_CT"
    sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],list(df_vs.columns))
    conn.cursor().execute(sql_generator.GenerateDDL())
    load.SfWrite_PandaDF(conn,params,df_vs)
    
    # upload acs fields data
    params["tgt_table"] = "ACS_FIELDS"
    sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],params["tgt_table"],list(df_col.columns))
    conn.cursor().execute(sql_generator.GenerateDDL())
    load.SfWrite_PandaDF(conn,params,df_col)

utils.pyclean()