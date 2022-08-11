'''
Monthly NPPES Data Updates
'''
import os
import json
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import load
import extract
import pandas as pd
import shutil
# or: requests.get(url).content

dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))

# download and extract files in a temporary location
resp = urlopen(config_data["nppes_keys"]["url"])
zipped_file = ZipFile(BytesIO(resp.read()))
zipped_file.extractall('tmp_dir') # tmp_dir will be created under root folder
file_lst = zipped_file.namelist()
header_file_name = [x for x in file_lst if 'npidata' in x and 'FileHeader' in x]
body_file_name = [x for x in file_lst if 'npidata' in x and 'FileHeader' not in x]
'''
file_lst = os.listdir('tmp_dir')
header_file_name = [x for x in file_lst if 'npidata' in x and 'FileHeader' in x][0]
body_file_name = [x for x in file_lst if 'npidata' in x and 'FileHeader' not in x][0]
'''
# write useful csv files from temp folder to snowflake
user = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["user_secret"])
pwd = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["pwd_secret"])
acct = load.AWSSecrets(region_name=config_data["aws"]["region"], secret_name = config_data["aws"]["acct_secret"])
table_name = "NPIDATA"
 
snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
    params = config_data["snowflake_c2p_stg"]
    # set up the snowflake environment
    load.SfExec_EnvSetup(conn.cursor(),params)
    # assume the file structure doesn't change over time, then there are three NPPES tables can be consumed
    # a. npidata... - main table; b. other_name...; c) endpoint...
    # generate table shell
    meta = pd.read_csv(f'tmp_dir/{header_file_name}', index_col=0, nrows=0).columns.tolist()
    sql_generator = extract.SqlGenerator_HeaderURL(params["env_schema"],table_name,meta)
    conn.cursor().execute(sql_generator.GenerateDDL())
    # direct write data to table shell
    load.SfExec_WriteCSV(conn.cursor(),"tmp_dir",body_file_name,table_name)

# remove temp folder after writing all csv files to snowflake
try:
    shutil.rmtree("tmp_dir")
except OSError as e:
    print ("Error: %s - %s." % (e.filename, e.strerror))