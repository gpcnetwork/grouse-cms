/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_stg_dml.sql                                                 
# Desccription: DML script for populating intermediate staging table
*/

call stage_obs_comm('PUBLIC_DATA_STAGING','RUCA_TR');
call stage_obs_comm('PUBLIC_DATA_STAGING','ACS_TR');
call stage_obs_comm('PUBLIC_DATA_STAGING','ADI_BG');
