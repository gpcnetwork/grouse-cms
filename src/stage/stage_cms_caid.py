#####################################################################     
# Copyright (c) 2021-2022 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: stage_cms_caid.py                                                 
# The file read Snowflake credential from secret manager and establish
# database connection using python connector; and send DML script 
# over to snowflake to perform data staging steps                                              
#####################################################################
# BEFORE YOU START, 
# a. INSTALL DEPENDENCIES BY RUNNING ./dep/setup.sh 
# b. MAKE SURE extract.py, load.py, utils.py ARE LOADED UNDER THE SAME DIRECTORY
#####################################################################
import os
from re import match, sub
from utils import get_objects
from extract import *
from load import *

"""
TODO!!!
within snowflake connection context, perform data staging process
1. Create a single-column fixed-width table
2. Copy .csv file from established snowflake "Stage" into predefined table shell
3. Construct DDL and "Substr" statements using "FTSParser" class and execute it in snowflake 
"""
