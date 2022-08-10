
##################################################################### 
# Copyright (c) 2021-2022 University of Missouri  
# Author: Askar Afshar, as9bn@missouri.edu                         
# File: lambda_function.py                                                 
# A lambda function to extract small size (~1GB or less) zip files
# Instruction; https://github.com/Missouri-BMI/GROUSE/issues/48.                                             
#####################################################################
from io import BytesIO
import os
import boto3
import zipfile
import traceback
import logging


def bucket_list():
    '''# Returns a list of all buckets owned by the authenticated sender of the request.
    '''
    s3_client = boto3.client('s3')
    buckets = s3_client.list_buckets()
    bucket_names = []
    for bucket in buckets['Buckets']:
        bucket_names.append(bucket["Name"])
    return bucket_names
    
def get_objects(bucket_name):
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
        if filename != '':
            if path not in filenames:
                filenames[path] = [filename]
            else:
                filenames[path].append(filename)
    return filenames
    
def files_to_unzip():
    '''
    #return: dictionary 
           # keys: name of buckets, which contain a single zip file
           # values: list of two elemens; 1) prefix 2) key
    '''
    zipped_path_keys = {} # key: bucket, value: [prefix,key]
    s3 = boto3.resource('s3')
    buckets = bucket_list()
    
    for bucket_name in buckets:
        bucket = s3.Bucket(bucket_name)
        count_obj = sum(1 for _ in bucket.objects.all())
        if count_obj != 0:
            bucket_contents = get_objects(bucket_name)
            #print(bucket_contents)
            
            for key,value in bucket_contents.items():
                if len(value)==1 and value[0].endswith('.zip') == True: # IFF a folder contains one zip file
                    zipped_path_keys[bucket_name] = [key,value[0]]
                    
    return zipped_path_keys


    
def lambda_handler(event, context):
    try:
        s3_resource = boto3.resource('s3')
        zipped_path_keys = files_to_unzip() #{'nextgenbmi-cms-test': ['', 'CDM_20220203_1.zip']}

        for key,value in zipped_path_keys.items():
            file_path = ''
            if value[0] == '':
                zip_obj = s3_resource.Object(bucket_name=key, key=value[1])
            else:
                zip_obj = s3_resource.Object(bucket_name=key, key=value[0]+"/"+value[1])
                file_path = value[0]
            buffer = BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)
            for filename in z.namelist():
                f_name = f'{filename}'
                if file_path != '':
                    f_name = file_path+"/"+f_name
                file_info = z.getinfo(filename)
                s3_resource.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=key,
                    Key= f_name)
            print("Zip Files Are Extracted! \n Done!")
    except Exception as e:
        logging.error(traceback.format_exc())
