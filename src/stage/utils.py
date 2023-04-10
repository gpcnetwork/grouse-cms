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
import gzip
from dataclasses import dataclass
from abc import ABC, abstractmethod

def get_objects(bucket_name,subfolder='',s3_client=None)->dict:
    """
    bucket_name: string
    returns a dictionary: key: folder name, value: list of object names
    """
    if s3_client is None:
        s3_client = boto3.client('s3') # Using the default session
    # https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=bucket_name, 
        Prefix=subfolder
    )
    filenames = {}
    mod_dates = []
    for page in pages:
        for file in page['Contents']:
            # Iterate over the content of the bucket and retreive folders and contents
            path, filename = os.path.split(file['Key'])
            mod_dates.append(file['LastModified'].strftime('%Y%m%d'))
            if filename != '':
                if path not in filenames:
                    filenames[path] = [filename]
                else:
                    filenames[path].append(filename)
    # attach modified dates
    filenames['modified_date'] = mod_dates
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

@dataclass
class zipped_file_s3(ABC):
    """representation of a zipped file object in s3 bucket"""
    src_bucket:str
    src_key:str
    tgt_bucket:str
    tgt_prefix:str

    @abstractmethod
    def unzip_and_upload(self,verb=True)->None:
        """unzip file on local disk and upload to target location in s3"""
        if verb:
            print("unzip file from ",f'{self.src_bucket}/{self.src_key}'," to ",f'{self.tgt_bucket}/{self.tgt_prefix}')
    
    def download_s3obj(self):
        zipname = self.src_key.rsplit('/',1)[-1]
        s3 = boto3.client('s3')
        with open(zipname, 'wb') as data:
            s3.download_fileobj(
                Bucket  = self.src_bucket, 
                Key = self.src_key,
                Fileobj = data
            )
        return s3

@dataclass
class zipped_zip(zipped_file_s3):
    """files compressed using conventional zip technique"""
    # https://medium.com/@johnpaulhayes/how-extract-a-huge-zip-file-in-an-amazon-s3-bucket-by-using-aws-lambda-and-python-e32c6cf58f06
    def unzip_and_upload(self):
        # read zip file into a BytesIO buffer object
        s3_resource = boto3.resource('s3')
        zip_obj = s3_resource.Object(
            bucket_name=self.src_bucket, 
            key=self.src_key
        )
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        # upload individual zipped file into a target bucket
        z = zipfile.ZipFile(buffer)
        for filename in z.namelist():
            file_info = z.getinfo(filename)
            tgt_key = f'{self.tgt_prefix}/{filename}'
            s3_resource.meta.client.upload_fileobj(
                z.open(filename),
                Bucket=self.tgt_bucket,
                Key=tgt_key
            )
        # return extracted file names
        return z.namelist() 

@dataclass
class zipped_gz(zipped_file_s3):
    """files compressed using gzip technique"""
    def unzip_and_upload(self):
        # download
        zipname = self.src_key.rsplit('/',1)[-1]
        s3 = self.download_s3obj()
        # unzip, assume single file
        with gzip.open(zipname, 'rb') as f_in:
            filename = zipname.rsplit(".",1)[0]
            with open(filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        # upload
        tgt_key = f'{self.tgt_prefix}/{filename}'
        s3.upload_file(
            Filename = filename,
            Bucket=self.tgt_bucket,
            Key=tgt_key
        )
                
        # return extracted file names
        return [filename] 

@dataclass
class zipped_7z(zipped_file_s3):
    """files compressed using gzip technique"""
    def unzip_and_upload(self):
        zipname = self.src_key.rsplit('/',1)[-1]
        s3 = self.download_s3obj()
        # collect all file names
        with py7zr.SevenZipFile(zipname, 'r') as zip:
            allfiles = zip.getnames()
        # extract and upload
        with py7zr.SevenZipFile(zipname, 'r') as zip:
            for filename in allfiles:
                zip.extract(targets=filename)
                tgt_key = f'{self.tgt_prefix}/{filename}'
                s3.upload_file(
                    Filename = filename,
                    Bucket=self.tgt_bucket,
                    Key=tgt_key
                )
        # return extracted file names
        return allfiles 

def pyclean():
    os.popen('find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf')

def format_bucket(bucket_name,str_json)->list:
    """
    bucket_name: target bucket where subfolder structure needs to be formed
    folder_json: json file describing subfolder structure, in nested list of dict format
    - [{"type":"","name":"","children":[{},{},...]},{"type":"","name":"","children":[]},...]
    https://stackoverflow.com/questions/1939743/amazon-s3-boto-how-to-create-a-folder
    """
    def path_constr(str_dict):
        key_paths = []
        def path_constr_dfs(str_dict,key_path,key_paths):
            key_path+=f'{str_dict["name"]}/'
            if(str_dict["type"]=="leaf"):
                key_paths.append(key_path)
            # recurrsion
            for child_dict in str_dict["children"]:
                path_constr_dfs(child_dict,key_path,key_paths)
        path_constr_dfs(str_dict,'',key_paths)
        return(key_paths)
    
    keypaths = [] 
    s3 = boto3.client('s3')
    for rchild in str_json:
        # dfs to construct path
        r_keypath = path_constr(rchild)
        for keypath in r_keypath:
            # check if the folder key exists
            # https://github.com/boto/boto3/issues/1361
            try:
                s3.head_object(Bucket=bucket_name,Key=keypath)
                continue
            except ClientError as e:
                # boto3 function to create folder object
                s3.put_object(Bucket=bucket_name,Key=keypath)
                keypaths.append(keypath)
    return(keypaths)

def copy_file_to_folder(bucket_name,src_file,tgt_folder,tgt_file=None,verb=True):
    """
    bucket_name: s3 bucket needs to be organized
    src_file: source file; 
      - could be either full name of a single file (contains '.'), or 
      - folder name (must end with '/'), or
      - file name pattern directly under bucket (e.g., zip)
    tgt_folder: target folder;
    tgt_file: to change file name in target folder. NOTE this is only allowed when copying a single file
    """
    if(src_file.endswith('/')):
        # input is a folder
        file_lst = get_objects(bucket_name,subfolder=src_file[:-1])['']
        file_lst = [f'{src_file}{x}' for x in file_lst]
    elif('.' not in src_file and '/' not in src_file):
        # input is a file type or name pattern
        file_lst = get_objects(bucket_name)['']
        file_lst = [x for x in file_lst if src_file in x]
    elif('.' in src_file):
        # input is a single file
        file_lst = []
        file_lst.extend([src_file])
    else:
        exit("src_file should either contain '.' for single file or end with '/' for folder or '*.<file-type>' for file type!")
    # s3 bucket copy 
    s3 = boto3.resource('s3')
    for key in file_lst:
        # https://stackoverflow.com/questions/47468148/how-to-copy-s3-object-from-one-bucket-to-another-using-python-boto3
        copy_source = {
              'Bucket': bucket_name,
              'Key': key
        }
        file_key = key.rsplit('/', 1)[-1]
        if tgt_file is not None:
            file_key = tgt_file
        tgt_key = f'{tgt_folder}{file_key}'
        s3.meta.client.copy(
            copy_source,
            bucket_name,
            tgt_key
        )
        if(verb): print(f'{key} copied to {tgt_key}')
