##################################################################### 
# Copyright (c) 2021-2026 University of Missouri  
# Author: Askar Afshar, as9bn@umsystem.edu    
# Modified by: Xing Song, xsm7f@umsystem.edu
# Modified date: 10/14/2022
# File: unzip.py                                                 
# The script interates over all specified bucket in S3, finds the buckets 
# that contain only one zip file and extract the content.
#####################################################################
# pip3 install boto3
# sudo amazon-linux-extras install epel
# sudo yum install p7zip -y
# Increase EBS volume by ./resize.sh INT command
##  1) download resize.sh into EC2 environment, 
##  2) run chmod +x resize.sh 
##. 3) run ./resize.sh SIZE(in integer)
####################################################################
from io import BytesIO
import os
import boto3
from botocore.exceptions import ClientError
import zipfile
import traceback
import logging
import sys
import time

def get_objects(bucket_name):
    '''
    bucket_name: string
    returns a dictionary: 
    - key: folder name
    - value: list of object names
    '''
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
    

def files_to_unzip(bucket_name,file_key):
    '''
    return: dictionary 
    - keys: name of buckets, which contain a single zip file
    - values: list of two elemens; 1) prefix 2) key
    '''
    zipped_path_keys = {} # key: bucket, value: [prefix,key]
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    count_obj = sum(1 for _ in bucket.objects.all())
    if count_obj != 0:
        bucket_contents = get_objects(bucket_name)
        for key,value in bucket_contents.items():
            zfile = [x for x in value if x.endswith('.zip') and file_key in x]
            if len(zfile) > 0:
                zipped_path_keys[bucket_name] = [key,zfile[0]]
    return zipped_path_keys


def getListOfFiles(dirName):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles

try:
    zipped_path_keys = files_to_unzip('gpc-allina-upload','mapping')
    current_dir = os.getcwd() 
    directory = 'extract'
    path = os.path.join(current_dir, directory)

    if os.path.isdir(path) != True:
        os.mkdir(path)
    print("Directory for extracted files: ",path)
    
    s3 = boto3.client('s3')
    for key,value in zipped_path_keys.items():
        with open(value[1], 'wb') as f:
            print("Downloading... ",value[1])
            s3.download_fileobj(key, value[1], f)
            f.close()
        print("Extracting... ",value[1])
        
        cmd = '7za x \''+value[1]+'\' -o'+path # command to extract zip file into path (directory)
        os.system(cmd) # run command
        print(value[1], "Extracted... \n")
        with zipfile.ZipFile(value[1], 'r') as zipObj:
            listOfiles = zipObj.namelist()
            print("Files to upload into S3: \n",listOfiles)
            zipObj.close()

        files_path = getListOfFiles(path) # list of path-files

        split_path_files = {} # seperate path and files
        for i in files_path:
            if os.path.split(i)[0] not in split_path_files:
                split_path_files[os.path.split(i)[0]]=[os.path.split(i)[1]]
            else:
                split_path_files[os.path.split(i)[0]].append(os.path.split(i)[1])

        for path,files in split_path_files.items(): #
            for file in files:
                print("Uploading... ",file)
                response = s3.upload_file(path+"/"+file, key, "extract/"+file)

        time.sleep(300)
        os.system("rm *.zip")
        os.system("rm -rf extracted_files/"+"*")

    print('Done!')
except Exception as e:
    logging.error(traceback.format_exc())


