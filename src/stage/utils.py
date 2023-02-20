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

def get_objects(bucket_name,subfolder=None,s3_client=None)->dict:
    """
    bucket_name: string
    returns a dictionary: key: folder name, value: list of object names
    """
    if s3_client is None:
        s3_client = boto3.client('s3') # Using the default session
    response = s3_client.list_objects(Bucket=bucket_name)
    # Iterate over the content of the bucket and retreive folders and contents
    request_files = response["Contents"]
    filenames = {}
    mod_dates = []
    for file in request_files:
        path, filename = os.path.split(file['Key'])
        mod_dates.append(file['LastModified'].strftime('%Y%m%d'))
        if filename != '':
            if path not in filenames:
                filenames[path] = [filename]
            else:
                filenames[path].append(filename)
    # flattened
    if subfolder:
        filenames_cp = filenames
        mod_dates_cp = mod_dates
        filenames = {}
        filenames[''] = []
        mod_dates = []
        idx_counter = 0
        for key, val in filenames_cp.items():
            if key.startswith(subfolder):
                mod_dates.append(mod_dates_cp[idx_counter])
                filenames[''].extend(val)
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

# https://medium.com/@johnpaulhayes/how-extract-a-huge-zip-file-in-an-amazon-s3-bucket-by-using-aws-lambda-and-python-e32c6cf58f06
def unzip_file(src_bucket:str,src_key:str,tgt_bucket:str,tgt_prefix:str,verb=True)->None:
    # read zip file into a BytesIO buffer object
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=src_bucket, key=src_key)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())
    # upload individual zipped file into a starget bucket
    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        file_info = z.getinfo(filename)
        tgt_key = f'{tgt_prefix}/{filename}'
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=tgt_bucket,
            Key=tgt_key
        )
        if verb:
            print("unzip file from ",f'{src_bucket}/{src_key}'," to ",f'{tgt_bucket}/{tgt_key}')

# def unzip_file_7z():
#     # sudo yum install p7zip -y
#     def getListOfFiles(dirName):
#     # create a list of file and sub directories 
#     # names in the given directory 
#     listOfFile = os.listdir(dirName)
#     allFiles = list()
#     # Iterate over all the entries
#     for entry in listOfFile:
#         # Create full path
#         fullPath = os.path.join(dirName, entry)
#         # If entry is a directory then get the list of files in this directory 
#         if os.path.isdir(fullPath):
#             allFiles = allFiles + getListOfFiles(fullPath)
#         else:
#             allFiles.append(fullPath)
                
#     return allFiles

#     try:
#         zipped_path_keys = files_to_unzip('gpc-allina-upload','mapping')
#         current_dir = os.getcwd() 
#         directory = 'extract'
#         path = os.path.join(current_dir, directory)
    
#         if os.path.isdir(path) != True:
#             os.mkdir(path)
#         print("Directory for extracted files: ",path)
        
#         s3 = boto3.client('s3')
#         for key,value in zipped_path_keys.items():
#             with open(value[1], 'wb') as f:
#                 print("Downloading... ",value[1])
#                 s3.download_fileobj(key, value[1], f)
#                 f.close()
#             print("Extracting... ",value[1])
            
#             cmd = '7za x \''+value[1]+'\' -o'+path # command to extract zip file into path (directory)
#             os.system(cmd) # run command
#             print(value[1], "Extracted... \n")
#             with zipfile.ZipFile(value[1], 'r') as zipObj:
#                 listOfiles = zipObj.namelist()
#                 print("Files to upload into S3: \n",listOfiles)
#                 zipObj.close()
    
#             files_path = getListOfFiles(path) # list of path-files
    
#             split_path_files = {} # seperate path and files
#             for i in files_path:
#                 if os.path.split(i)[0] not in split_path_files:
#                     split_path_files[os.path.split(i)[0]]=[os.path.split(i)[1]]
#                 else:
#                     split_path_files[os.path.split(i)[0]].append(os.path.split(i)[1])
    
#             for path,files in split_path_files.items(): #
#                 for file in files:
#                     print("Uploading... ",file)
#                     response = s3.upload_file(path+"/"+file, key, "extract/"+file)
    
#             time.sleep(300)
#             os.system("rm *.zip")
#             os.system("rm -rf extracted_files/"+"*")
    
#         print('Done!')
#     except Exception as e:
#         logging.error(traceback.format_exc())

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
