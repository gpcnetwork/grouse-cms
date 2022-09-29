##################################################################### 
# Copyright (c) 2021-2022 University of Missouri  
# Author: Askar Afshar, as9bn@missouri.edu                         
# File: decrypt.py                                                 
# The file takes AWS bucket objects and corresponding decryption   
# passwords from AWS secret manager, outputs decrypted files into 
# another AWS bucket.                                             
#####################################################################
# BEFORE YOU START, INSTALL FOLLOWING PACKAGES #
# 1. pip3 install boto3
# 2. pip3 install matplotlib
# 3. sudo yum install glibc.i686
#####################################################################
import os
import time
import boto3
import base64
from botocore.exceptions import ClientError
import pexpect
import logging
import ast
import matplotlib.pylab as plt
import csv
import logging  
import json
from uuid import uuid4
from datetime import datetime
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)

''' helper functions
- If the bucket does not exist, create one with the 'create_bucket()' function
- To check the list of the buckets, use 'bucket_list()' fuction
- To get the bucket keys (folder and subfolders), use 'key('bucke-name-here')' function
- To get the bucket folders and objects list, use get_objects('bucke-name-here') fuction
- To retreive secret passwords from aws secret manager, use get_secret_password(bucket_name) function
- To get the object size, use object_length(bucket_name, key) function
- To get the list of files in a directory with a specified extention, use get_file('dat')
- To upload a file to a s3 bucket, use upload_file(file_name, bucket, object_name) function.
'''

#session = boto3.session.Session()
#credentials = session.get_credentials()
#ACCESS_KEY = credentials.access_key  
#SECRET_KEY = credentials.secret_key

# session duration
TTL = 43200  # max


class BotoSession:
    """
    Boto Helper class which lets us create refreshable session, so that we can cache the client or resource.
    source: https://pritul95.github.io/blogs/boto3/2020/08/01/refreshable-boto3-session/
    
    Usage
    -----
    session = BotoSession().refreshable_session()

    client = session.client("s3") # we now can cache this client object without worrying about expiring credentials
    """

    def __init__(
        self,
        region_name: str = None,
        profile_name: str = None,
        sts_arn: str = 'give role arn',
        session_name: str = 'give session name',
        service_name: str = 'sts'
    ):
        """
        Initialize `BotoSession`

        Parameters
        ----------
        region : str 
            Default region when creating new connection.

        profile_name : str (optional)
            The name of a profile to use.

        sts_arn : str 
            The role arn to sts before creating session.

        session_name : str 
            An identifier for the assumed role session. 
        """

        self.region_name = region_name
        self.profile_name = profile_name
        self.sts_arn = sts_arn
        self.service_name = service_name
        # read why RoleSessionName is important https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html
        self.session_name = session_name or uuid4().hex

    def __get_session_credentials(self):
        """
        Get session credentials
        """
        credentials = {}
        session = boto3.Session(region_name=self.region_name, profile_name=self.profile_name)

        sts_client = session.client(self.service_name, region_name=self.region_name)
        response = sts_client.assume_role(
                RoleArn=self.sts_arn,
                RoleSessionName=self.session_name,
                DurationSeconds=TTL,
            ).get("Credentials")

        credentials = {
                "access_key": response.get("AccessKeyId"),
                "secret_key": response.get("SecretAccessKey"),
                "token": response.get("SessionToken"),
                "expiry_time": response.get("Expiration").isoformat(),
            }

        return credentials
    
    def refreshable_session(self) -> boto3.Session:
        """
        Get refreshable boto3 session.
        """
        try:
            # get refreshable credentials
            refreshable_credentials = RefreshableCredentials.create_from_metadata(
                metadata=self.__get_session_credentials(),
                refresh_using=self.__get_session_credentials,
                method="sts-assume-role",
            )

            # attach refreshable credentials current session
            session = get_session()
            session._credentials = refreshable_credentials
            session.set_config_variable("region", self.region_name)
            autorefresh_session = Session(botocore_session=session)

            return autorefresh_session

        except:
            return boto3.Session()


#created bucket to write decrypted data into it
def create_bucket(bucket_name, region_name):
    '''
    bucket_name: string
    '''
    s3 = boto3.client('s3')
    res = s3.create_bucket(Bucket=bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': region_name})
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    if bucket.creation_date:
        return True
    else:
        return False
       
#print(create_bucket('your-bucket-name','region'))

def bucket_list():
    '''
    # Returns a list of all buckets owned by the authenticated sender of the request.
    '''
    s3_client = boto3.client('s3')
    buckets = s3_client.list_buckets()
    bucket_names = []
    for bucket in buckets['Buckets']:
        bucket_names.append(bucket["Name"])
    return bucket_names
   
#print("Printing buckets list: \n", bucket_list())

# Prints bucket's keys
def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return list(_.key for _ in bucket.objects.filter(Prefix=prefix))
 
#print(keys("nextgenbmi-cms-test"))

def get_objects(bucket_name,filter_keys,client):
    '''
    bucket_name: string
    returns a dictionary: key: folder name, value: list of object names
    '''
    # Returns some or all (up to 1,000) of the objects in a bucket.
    s3_client = client #boto3.client('s3') # Using the default session
    response = s3_client.list_objects(Bucket=bucket_name)
    # Iterate over the content of the bucket and retreive folders and contents
    request_files = response["Contents"]
    filenames = {}
    for file in request_files:
        path, filename = os.path.split(file['Key'])
        if filename != '' and filename.endswith('.txt') != True and path in filter_keys:
            if path not in filenames:
                filenames[path] = [filename]
            else:
                filenames[path].append(filename)
    return filenames

#filenames = get_objects('nextgenbmi-cms-test')
#print(filenames)

def get_secret_password(bucket_name, filter_keys,session,client,region_name):  
    '''
    bucket_name: dictionary, created by
    returns dictionary: key is filename, value is secret password
    '''
    session = session #boto3.session.Session()
    client_sm = session.client(service_name='secretsmanager',region_name=region_name)
    filenames = get_objects(bucket_name, filter_keys,client)
   
    #build secret names from folder names in s3 bucket
    secret_names = []
    for key,values in filenames.items():
        secret_names.append('cms-'+key+'-key')
    # Gets secret keys by secret names (sotred in the secret manager AWS)
    secrets = []
    try:
        for secret_name in secret_names:
            get_secret_value_response = client_sm.get_secret_value(SecretId=secret_name)
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            secrets.append(secret)
    except Exception as e:
        raise Exception("Unexpected error in get secret values: " + e.__str__())
   
    #secrets: ['{"cms-R5900-key":"pass"}', '{"cms-7209-key":"pass"}']
    secret_ls= []
    for i in secrets:
        secret_ls.append(ast.literal_eval(i))
    pass_dict = {}
    for dictt in secret_ls:
        pass_dict.update(dictt)
   
    return pass_dict
#pass_dict: {'cms-R5900-key': 'pass', 'cms-7209-key': 'pass'}

def object_length(bucket_name, key,client):
    '''
    bucket_name: string
    key: string
    return int size (MB) of the object
    '''
    s3_client = client#boto3.client('s3')
                     
    response = s3_client.head_object(Bucket=bucket_name,Key=key)
    size = response['ContentLength']
    return (size/(1024*1024))
   
def get_file(extension):
    '''
    extension: str
    return list: file names 
    '''
    file_names = []
    path=os.getcwd()
    for i,file in enumerate(os.listdir(path)):
        if file.endswith("."+extension):
            file_names.append(os.path.basename(file))
    return file_names

def get_all_files():
    '''
    return list: all file names
    '''
    file_names = []
    path=os.getcwd()
    for i,file in enumerate(os.listdir(path)):
        if file != 'enc_file':
            file_names.append(os.path.basename(file))
    return file_names

def upload_file(file_name, bucket,client,object_name=None):
    '''
    file_name: str
    bucket: str
    object_name: str
    return True/False
    '''
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    #s3_client = boto3.client('s3')
    s3_client = client#boto3.client('s3')

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def write_dict(dictionary,root_dir,name):
    '''
    dictionay: key,vale
    '''
    with open(root_dir+'/'+name+'.csv', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in dictionary.items():
           writer.writerow([key, value])
        csv_file.close()

def bucket_size(bucket_name):
    '''
    bucket_name: str
    return size int
    '''
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    size = sum(1 for _ in bucket.objects.all())
    return size
    
if __name__ == '__main__':

    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))
    
    def promt_user():
        read_bucket = config_data["cms_keys"]["s3_bucket_source"]
        write_bucket = config_data["cms_keys"]["s3_bucket_target"]
        print("\n The Bucket You Are Reading From Is : ", read_bucket)
        print("\n The Bucket You Are Writing To Is : ", write_bucket)
        return [read_bucket,write_bucket]
        
    def plot_runningtime(run_dict):    
        # casting string key, value to float and round them up to two decimal points.
        running_time = {round(float(k),2):round(float(v),2) for k,v in run_dict.items()}
        # sort the dict based on key (size of the files)
        run_time_sorted = sorted(running_time.items(), key=lambda s: s[0])
        x, y = zip(*run_time_sorted) # unpack a list of pairs into two tuples
        
        plt.plot(x, y,'o', color='blue')
        plt.xlabel("File size (MB)")
        plt.ylabel("Time (sec)")
        plt.savefig('run_time.png')
        
    def main():
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        config_data = json.load(open(file=f'{dir_path}/config.json',encoding = "utf-8"))    
        session = BotoSession(region_name=config_data["aws_grouse_default"]["region"]).refreshable_session()
        client = session.client("s3") 
        #credentials = session.get_credentials()
        #ACCESS_KEY = credentials.access_key  
        #SECRET_KEY = credentials.secret_key
        #print(ACCESS_KEY)
        #print(SECRET_KEY)
        READ_BUCKET, WRITE_BUCKET = promt_user()
        # get root directory
        root_dir = os.getcwd()
        # create a data directory
        t = time.time()
        #print(t)
        if not os.path.isdir('data'):
            os.mkdir("./data")
        # step into data directory
        os.chdir("./data")
           
        # s3 buckets which contain executable objects
        filter_keys = config_data["cms_keys"]["cms_file_keys"][0] #LIFO
        #keys_ elements allow to access the content
        keys_ = []
        filenames = get_objects(READ_BUCKET,filter_keys,client)
        #print(filenames)
        for key,value in filenames.items():
            if key in filter_keys:
                for i in value:
                    keys_.append(key+"/"+i)
        #keys_: 'R5900/res000050354req005900_2011_HSPC_SPAN'

        # get passwords
        pass_dict = get_secret_password(READ_BUCKET,filter_keys,session,client,config_data["aws_grouse_default"]["region"])

        content_size = {} # collects size of aws objects/files
        for key in keys_:
            size = object_length(READ_BUCKET, key,client)
            content_size[key] = size
            
        content_size = dict(sorted(content_size.items(), key=lambda x: x[1], reverse=True))
        sorted_keys = list(content_size.keys())
        #print("Number of keys: ", len(sorted_keys))
        running_time = {} # key: file, value: time(sec)
        all_files_decrypted = {}
        decrypt_count = 0
        #all_obj_size = bucket_size(READ_BUCKET)
       # print("\nYou Have %d Objects In %s Bucket In Total.\n" % (all_obj_size, READ_BUCKET) )

        for file in sorted_keys:
            start = time.time()
            
            log.info('Decrypting %s' % file)

            os.system("aws s3api get-object --bucket "+READ_BUCKET+ " --key "+file+" "+"enc_file")

            os.system("chmod -R a+rwx enc_file")
            cmd = "./enc_file"
            k = file.split('/')[0]
            k = "cms-"+k+"-key"
            password = pass_dict[k]

            child = pexpect.spawn(cmd)
            child.sendline(password)

            while child.isalive()==True:
                time.sleep(1)

            all_files_decrypted[file] = get_all_files()
            for dat_file in get_file('dat'):
                log.info('Uploading %s file' % dat_file)
                upload_file(dat_file, WRITE_BUCKET,client, 'CMS/dat_files/'+dat_file)
            for fts_file in get_file('fts'):
                log.info('Uploading %s file' % fts_file)
                upload_file(fts_file, WRITE_BUCKET,client, 'CMS/fts_files/'+fts_file)

            os.system("rm -r *")
            end = time.time()
            delta = end - start
            running_time[file] = delta
            
            decrypt_count += 1
            log.info('Decrypted %d files' % decrypt_count)
            
            
        write_dict(running_time,root_dir,'running_time')
        write_dict(content_size,root_dir,'content_size')
        write_dict(all_files_decrypted,root_dir, 'all_files_decrypted')# writes the dictionary into root folder
        #plot_runningtime(running_time)

    main()
