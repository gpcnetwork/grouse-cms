/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: obs_comm_dml.sql                                                 
# Desccription: DML script for populating final CDM tables
*/

call transform_to_obs_comm('RUCA_TR','TR');
call transform_to_obs_comm('ACS_TR','TR');
call transform_to_obs_comm('ADI_BG','BG');
