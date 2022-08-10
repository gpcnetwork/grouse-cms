import os
import boto3
from botocore.exceptions import ClientError
import logging
import requests
import pandas as pd
import io
import zipfile
import psutil
import time
import zipfile
import urllib

def get_objects(bucket_name,subfolder)->dict:
    """
    bucket_name: string
    returns a dictionary: key: folder name, value: list of object names
    """
    # Returns some or all (up to 1,000) of the objects in a bucket.
    s3_client = boto3.client('s3') # Using the default session
    response = s3_client.list_objects(Bucket=bucket_name)
    # Iterate over the content of the bucket and retreive folders and contents
    request_files = response["Contents"]
    filenames = {}
    for file in request_files:
        path, filename = os.path.split(file['Key'])
        if filename != '' and filename.endswith('.txt') != True and filename.endswith('.csv') != True:
            if path not in filenames:
                filenames[path] = [filename]
            else:
                filenames[path].append(filename)
    if subfolder:
        filenames_cp = filenames
        filenames = {}
        filenames[''] = []
        for key, val in filenames_cp.items():
            if key.startswith(subfolder):
                filenames[''].extend(val)
    return filenames

def load_meta_pcornet_url(url:str,sheet:str,tbl_col:str,var_col:str,dtype_col:str,exclude_raw_col=False)->dict:
    # get metadata content from the pcornet url link
    # url: https://pcornet.org/wp-content/uploads/2021/11/2021_11_29_PCORnet_Common_Data_Model_v6dot0_parseable.xlsx
    resp = requests.get(url)
    resp_df = pd.read_excel(resp.content,sheet_name=sheet)
    
    # force remove RAW columns
    if exclude_raw_col:
        resp_df = resp_df[~resp_df[var_col].str.startswith('RAW')]
    
    # # filter out RAW columns for better efficiencies
    # resp_df = resp_df[~resp_df[var_col].str.contains('RAW_')]
    
    # create a new column of lists with metadata info for each variable
    resp_df['data_type'] = resp_df[dtype_col].str.split('(', 1).str[0].str.split('SAS',1).str[1].str.strip().str.upper()
    resp_df['meta_col'] = resp_df[[var_col,'data_type']].values.tolist()
    resp_df = resp_df[[tbl_col,"meta_col"]]
    
    # convert into dictionary with table_name as key and [tbl_col,dtype_col] as val
    resp_dict = resp_df.groupby(tbl_col)['meta_col'].apply(list).to_dict()
    return(resp_dict)

def amend_metadata(old:list,new:dict)->list:
    old_keys = [i for i, j in old]
    meta = []
    for key in new.keys():
        key = key.upper()
        try:
            key_loc = old_keys.index(key) 
            meta.append(old[key_loc])
        except: 
            meta.append([key,'CHAR']) # data type char for all fields, as it is more generalizable
    return(meta)
    
# https://medium.com/@johnpaulhayes/how-extract-a-huge-zip-file-in-an-amazon-s3-bucket-by-using-aws-lambda-and-python-e32c6cf58f06
def unzip_large_file(src_bucket:str,file_name:str,tgt_bucket:str)->None:
    # read zip file into a BytesIO buffer object
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=src_bucket, key=file_name)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())
    # upload individual zipped file into a starget bucket
    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        file_info = z.getinfo(filename)
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=tgt_bucket,
            Key=f'{filename}'
        )

def get_benchmark()->list:
    # return the time in seconds since the epoch (the epoch is January 1, 1970, 00:00:00 (UTC)) 
    t = time.time()
    # get current available memory (negative number to)
    m = -1*psutil.virtual_memory().available*1e-6
    # get current free disk
    d = -1*psutil.disk_usage('/').free*1e-6
    return([t,m,d])

def download_zip(url:str,path_to_folder='default')->list:
    if path_to_folder == 'default':
        path_to_folder = f'{os.path.abspath(os.path.dirname(__file__))}/tmp_dir'
    resp = urllib.request.urlopen(url)
    zipped_file = zipfile.ZipFile(io.BytesIO(resp.read()))
    zipped_file.extractall(path_to_folder) # by default, it will be a tmp_dir under the same parent folder of this script
    file_lst = zipped_file.namelist()
    print(f'{file_lst} downloaded and unpacked!')
    

def pyclean():
    os.popen('find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf')