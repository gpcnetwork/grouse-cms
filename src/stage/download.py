import os
import json
import utils
import load

#specify other snowflake parameters (not sensitive)
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

#extract snowflake secrets from secret manager
user = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["user_secret"])
pwd = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["pwd_secret"])
acct = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["acct_secret"])
params = config_data["snowflake_download_finder_file"]

#create snowflake connection context
snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)
    
    # execute GET data
    conn.cursor().execute("SELECT * FROM ")