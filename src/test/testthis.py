'''
Test this script
'''
#load public package
import os,sys
import boto3
import botocore.session as bcs
import os
from smart_open import open as s3open
from re import findall, match, search, sub
import pandas as pd
import pyreadstat
from datetime import datetime,date,timedelta
from sas7bdat import SAS7BDAT

s3_client = boto3.resource('s3')
s3_bucket = s3_client.Bucket('gpc-mcri-upload')
s3_bucket.download_file(Key = 'extract/xwalk2cdm.csv',
                        Filename = 'xwalk2cdm.csv')

'''
#load custom package
sys.path.append(os.path.abspath(f'{os.path.dirname(os.path.dirname(__file__))}/stage'))
import utils,extract,load

sasdf, sasmeta = pyreadstat.read_sas7bdat('encounter.sas7bdat',row_offset=0, row_limit=1, metadataonly=True)
print(sasmeta)
'''

# sasdf, sasmeta = pyreadstat.read_sas7bdat('encounter.sas7bdat',row_offset=0, row_limit=10000000,
#                                           disable_datetime_conversion=True,
#                                           encoding = 'utf-8')
# print(sasdf.iloc[1:5,1:5])
# print(sasmeta.original_variable_types)

# def fix_datetime_cols(df, meta, tz = 'UTC'): 
#     cols = [col for col in df if col.lower().endswith('_date')]
#     for col in cols:
#         # some date columns may auto-convert to datetime64 formate
#         if 'DATETIME64' in meta.original_variable_types[col].upper():
#             df[col] = [x.date() for x in df[col].dt.tz_localize(tz)]
#         # some date columns may be converted to a 5-digit numbers representing numbers of days since 01/01/1960, SAS origin date
#         elif any(x in meta.original_variable_types[col].upper() for x in ['MMDDYY','YYMMDDN','DATE']):
#             df[col] = [date(1960,1,1) + timedelta(days=x) for x in df[col].fillna(999999)] #999999 convert missing dates to a future date 4697-11-26
#         # any other format remains the same until conversion error pops out when writing to snowflake
#         else:
#             df[col] = df[col]
#         # https://github.com/wesm/feather/issues/349
#         # https://stackoverflow.com/questions/32888124/pandas-out-of-bounds-nanosecond-timestamp-after-offset-rollforward-plus-adding-a
#         df[col] = pd.to_datetime(df[col],errors = 'coerce').dt.date #errored out any future dates (including the articial one)
    
#     cols = [col for col in df if col.lower().endswith('_time')]
#     for col in cols:
#         # some time column doesn't preserve the original HH:MM format
#         if any(x in meta.original_variable_types[col].upper() for x in ['HHMM','TIME']):
#             df[col] = [f'{str(timedelta(seconds=x))}' for x in df[col].fillna(0)] #fill na by 0 seconds
#         # any other format remains the same until conversion error pops out when writing to snowflake
#         else:
#             df[col] = df[col]
#     return(df)
    
# df=fix_datetime_cols(sasdf,sasmeta)
# print(df.iloc[1:5,1:5])

# with SAS7BDAT('harvestnew.sas7bdat') as r:
#     row = r.to_data_frame()
#     print(row)


'''
import multiprocessing
num_processes = multiprocessing.cpu_count()
print(num_processes)
'''

'''
cdm_meta = utils.load_meta_pcornet_url( url = 'https://pcornet.org/wp-content/uploads/2021/11/2021_11_29_PCORnet_Common_Data_Model_v6dot0_parseable.xlsx',
                                    sheet = 'FIELDS',
                                    tbl_col = 'TABLE_NAME',
                                    var_col = 'FIELD_NAME',
                                    dtype_col = 'SAS_DATA_TYPE')
'''

'''
print(cdm_meta['DISPENSING'])
                                    
sql_generator = extract.SqlGenerator_PcornetURL('PCORNET_CDM_UU','DISPENSING',cdm_meta['DISPENSING'])
print(sql_generator.GenerateDDL())
'''

'''
# s3_client = boto3.resource('s3')
# s3_client.meta.client.download_file('gpc-mcw-upload', 'encounter.sas7bdat', 'encounter.sas7bdat')

sasdf, sasmeta = pyreadstat.read_sas7bdat('lab_result_cm.sas7bdat',row_offset=0, row_limit=10000000)
print(sasdf.head())
# print(sasmeta.original_variable_types)
# print(sasmeta.readstat_variable_types )

'''

'''
new_meta = utils.amend_metadata(cdm_meta['ENCOUNTER'],sasmeta.original_variable_types)
print(cdm_meta['ENCOUNTER'])
print(new_meta)
'''

'''
def fix_date_cols(df, tz = 'UTC'): 
    cols = [col for col in df if col.lower().endswith('_date')]
    for col in cols:
        # some date columns may auto-convert to datetime64 formate
        if 'DATETIME64' in sasmeta.original_variable_types[col].upper():
            df[col] = [x.date() for x in df[col].dt.tz_localize(tz)]
        # some date columns may be converted to a 5-digit numbers representing numbers of days since 1st January 1970, which is the origin of Unix time
        elif 'MMDDYY' in sasmeta.original_variable_types[col].upper():
            df[col] = [date(1960,1,1) + timedelta(days=x) for x in df[col].fillna(999999)] #999999 convert missing dates to a future date 4697-11-26
        #    df[col] = df[col].replace({date(4697,11,26):''}) # reverse to missing dates
            # print(df[df['rx_order_date'] == date(4697,11,26)].index[0])
        # any other format remains the same until conversion error pops out when writing to snowflake
        else:
            df[col] = df[col]
        df[col] = pd.to_datetime(df[col],errors = 'coerce').dt.date #errored out any future dates
    return(df)

sasdf = fix_date_cols(sasdf)
print(sasdf.head())

# convert column names into upper cases
colnm_lower = sasdf.columns
sasdf.columns = [x.upper() for x in colnm_lower]
print(sasdf.head())


next_row, sasdf, sasmeta = load.Read_SAS7bDAT('encounter', num_processes = 1,
                                             row_offset = 0, row_limit = 10)

print(sasdf.head())
print(sasmeta)
'''


'''
sasdf = pd.read_sas('harvest.sas7bdat')
colnm_lower = sasdf.columns
sasdf.columns = [x.upper() for x in colnm_lower]
    
print(sasdf.dtypes)
'''

'''
print(date(1960,1,1) + timedelta(days=19884))

# SDAFileNameParser [variant test]
fts_file = extract.SDAFileNameParser("hospice_span_codes_file_res000050354_req005900_2011.fts")
print(fts_file.fname)
print(fts_file.stopwords)
print(fts_file.parse_fname())
#{'type': 'fts', 'req': '5900', 'year': '2011', 'tname': 'hospice_span_codes'}

fts_file = extract.SDAFileNameParser("bcarrier_claims_j_res000050354_req005900_2011_001.fts")
print(fts_file.fname)
print(fts_file.stopwords)
print(fts_file.parse_fname())
#{'type': 'fts', 'year': '2011', 'tname': 'bcarrier_claims_001'}


fts_file = extract.SDAFileNameParser("maxdata_ia_ip_2011.csv")
print(fts_file.fname)
print(fts_file.stopwords)
print(fts_file.parse_fname())
# {'type': 'csv', 'year': '2011', 'tname': 'maxdata_ia_ip'}
'''

'''
# FTSParser [unit test]
s3_bucket = "test-bucket-for-staging-cms-data"
s3_subfolder = ""
#s3_bucket = "nextgenbmi-snowpipe-master"
#s3_subfolder = "CMS/"  
filename = 'bcarrier_line_j_res000050354_req005900_2011'
#filename = 'mbsf_d_cmpnts_res000050354_req005900_2011'
filefts = s3open(f's3://{s3_bucket}/{s3_subfolder}fts_files/{filename}.fts')
fts_obj = extract.FTSParser(filefts)
parse_out = fts_obj.parse_body()
print(parse_out["rowsize_part"])
#[48679642, 48679642, 48679642, 48679642, 48679642, 48679637]
print(parse_out["rowsize"])
# [292077847]
print(parse_out["meta"])
# [['1', 'BENE_ID', 'BENE_ID', 'CHAR', '1', '15', 'Encrypted 723 Beneficiary ID'], 
#  ['2', 'CLM_ID', 'CLM_ID', 'CHAR', '16', '15', 'Encrypted Claim ID'], 
#  ['3', 'LINE_NUM', 'LINE_NUM', 'NUM', '31', '13', 'Claim Line Number'], 
#  ['4', 'NCH_CLM_TYPE_CD', 'CLM_TYPE', 'CHAR', '44', '2', 'NCH Claim Type Code']...

benchmk_test = []
benchmk_test.append([filename,
                    ','.join(str(i) for i in parse_out["colsize"]),
                    ','.join(str(i) for i in parse_out["rowsize"])+':'+','.join(str(i) for i in parse_out["rowsize_part"]),
                    ','.join(str(i) for i in parse_out["filesize"]),
                    10])
print(benchmk_test)
# [['bcarrier_line_j_res000050354_req005900_2011', '49', '292077847:48679642,48679642,48679642,48679642,48679642,48679637', '101643090756', 10]]
'''


'''
# SqlGenerator_FTS [unit test]
sql_generator = extract.SqlGenerator_FTS(filename,parse_out,"STAGING_TABLE")
gen_ddl = sql_generator.GenerateDDL()
gen_dml = sql_generator.GenerateDML()
gen_ddml = sql_generator.GenerateDDML()
print(gen_ddl)
print(gen_dml)
print(gen_ddml)
'''

'''
# SqlGenerator_PcornetURL [unit test]
meta_out = utils.load_meta_pcornet_url('https://pcornet.org/wp-content/uploads/2021/11/2021_11_29_PCORnet_Common_Data_Model_v6dot0_parseable.xlsx',
                                       'FIELDS',
                                       'TABLE_NAME',
                                       'FIELD_NAME',
                                       'SAS_DATA_TYPE',
                                       True)
print(meta_out)

sql_generator = extract.SqlGenerator_PcornetURL('SCRATCH','HARVEST',meta_out)
gen_ddl = sql_generator.GenerateDDL()
print(gen_ddl)

# SnowflakeConnection [unit test]
user = load.AWSSecrets(secret_name = "snowflake-grouse-user")
pwd = load.AWSSecrets(secret_name = "snowflake-grouse-pwd")
acct = load.AWSSecrets(secret_name = "snowflake-grouse-acct")

params = load.SnowflakeParams()
params.env_role = "GROUSE_ROLE_B_ADMIN"
params.env_wh = "GROUSE_WH"
params.env_db = "GROUSE_DB"
params.stg_stage = 'S3_EXT_STAGE'
params.stg_table = "STAGE_TABLE"
params.tgt_schema = 'SCRATCH'
params.tgt_table = 'HARVEST'

snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
#file_name = 'outpatient_demo_codes_res000050354_req007209_2015'

with snowflake_conn as conn:
    load.SfExec_EnvSetup(conn.cursor(),params)
    #load.SfExec_CreateFixedWidthTable(conn,params)
    #load.SfExec_CopyIntoDat(conn,params,file_name)
    conn.cursor().execute(gen_ddl)
    load.SfWriteTo_SASDat(conn,'test-gpc-mcw-upload','harvest',params)
'''

'''
# get_objects [unit test]
s3_bucket = "nextgenbmi-snowpipe-master"
s3_subfolder = "CMS/"
filenames = load.get_objects(s3_bucket)
for file_name in filenames[f"{s3_subfolder}dat_files"][16:17]:
    print(file_name)
'''

'''
# generate 1-to-n mapping between fts and dat files
s3_bucket = "test-bucket-for-staging-cms-data"
filenames = get_objects(awsparams.s3_bucket)
map_fts = [i for  y in filenames["dat_files"] for i, x in enumerate(filenames["fts_files"]) if match(x.split('.')[0],y.split('.')[0])]
print(filenames["dat_files"])
print(filenames["fts_files"])
print(map_fts)
'''

'''
params = load.SnowflakeParams()
params.stg_table = "STAGE_TABLE2"


s3_bucket = "nextgenbmi-snowpipe-master"
s3_subfolder = "CMS/"  
filenames = load.get_objects(s3_bucket)
map_fts = [i for  y in filenames[f"{s3_subfolder}dat_files"] for i, x in enumerate(filenames[f"{s3_subfolder}fts_files"]) if match(x.split('.')[0],y.split('.')[0])]


# print sql statment for single file from bucket
k = 205
for idx, val in enumerate(filenames[f"{s3_subfolder}dat_files"][k:(k+1)]):
    #print(val)
    #print(filenames[f"{s3_subfolder}fts_files"][map_fts[idx+k]])
    
    fts_filename = filenames[f"{s3_subfolder}fts_files"][map_fts[(idx+k)]]
    filefts = s3open(f's3://{s3_bucket}/{s3_subfolder}fts_files/{fts_filename}','r')
    dat_name = val.split('.')[0]
    fts_parse_out = extract.FTSParser(filefts).parse_body()
    print(fts_parse_out)
    
    sql_generator = extract.SqlGenerator(dat_name,fts_parse_out,params.stg_table)
    #print(sql_generator.GenerateDDL())
    #print(sql_generator.GenerateDML())
    print(sql_generator.GenerateDDML())
'''

"""
# load single file
user = load.AWSSecrets(secret_name = "snowflake-grouse-user")
pwd = load.AWSSecrets(secret_name = "snowflake-grouse-pwd")
acct = load.AWSSecrets(secret_name = "snowflake-grouse-acct")

params = load.SnowflakeParams()
params.env_role = "GROUSE_ROLE_B_ADMIN"
params.env_wh = "GROUSE_WH"
params.env_db = "GROUSE_DB"
params.stg_stage = 'S3_EXT_STAGE'
params.stg_table = "STAGE_TABLE"

snowflake_conn = load.SnowflakeConnection(user,pwd,acct) 
with snowflake_conn as conn:
        # set up the snowflake environment
        load.SfExec_EnvSetup(conn,params)
        load.SfExec_CreateFixedWidthTable(conn,params)
        
        # initialize benchmark params
        benchmk_data = []
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),"benchmark","benchmark_staging.csv")

        k = 0
        for idx, val in enumerate(filenames[f"{s3_subfolder}dat_files"][k:(k+1)]):
            fts_filename = filenames[f"{s3_subfolder}fts_files"][map_fts[(idx+k)]]
            filefts = s3open(f's3://{s3_bucket}/{s3_subfolder}fts_files/{fts_filename}','r')
            file_name = val.split('.')[0]
            fts_parse_out = extract.FTSParser(filefts).parse_body()
            
            sql_generator = extract.SqlGenerator(file_name,fts_parse_out,"STAGE_TABLE")

            load.SfExec_CopyIntoDat(conn,params,file_name)
"""

"""
# stitch file parts
ungroup_map = {}
for fname in filenames[f"{s3_subfolder}dat_files"]:
    fname_obj = extract.FileNameParser(fname.split('.')[0])
    if(match('.*_[0-9]{3}$',fname_obj.GetTableName)):
        ParentTable = sub('_[0-9]{3}$','',fname_obj.GetTableName)
        ungroup_map[f'{fname_obj.GetSchemaName}.{fname_obj.GetTableName}'] = f'{fname_obj.GetSchemaName}.{ParentTable}'
part_map = {n:[k for k in ungroup_map.keys() if ungroup_map[k] == n] for n in set(ungroup_map.values())}
#print(part_map)

for key, val_lst in part_map.items():
        sql_create = '''CREATE OR REPLACE TABLE %(table_name)s AS 
                       %(union_statements)s;''' % dict (
                    table_name = key,
                    union_statements = '\n UNION ALL \n'.join('SELECT * FROM %s' % item
                                                            for item in val_lst)
                    )
        sql_drop = ';\n'.join('DROP * TABLE %s' % item for item in val_lst)
print(sql_create)
print(sql_drop)
"""

"""
ungroup_map = {}
for fname in filenames[f"{s3_subfolder}dat_files"]:
    fname_obj = extract.FileNameParser(fname.split('.')[0])
    if(match('.*_[0-9]{3}$',fname_obj.GetTableName)):
        ParentTable = sub('_[0-9]{3}$','',fname_obj.GetTableName)
        ungroup_map[f'{fname_obj.GetSchemaName}.{fname_obj.GetTableName}'] = f'{fname_obj.GetSchemaName}.{ParentTable}'
        
#stitch multiple parts
benchmk_test=[]
part_map = {n:[k for k in ungroup_map.keys() if ungroup_map[k] == n] for n in set(ungroup_map.values())}
for key,parts in part_map.items():
    benchmk_test.append(["post-process",
                         f'stitch multi-part table:{key}',
                         f'{len(parts)} parts',
                         "",
                         10])
print(benchmk_test)
"""