# ==============================================================================
# Import Statements
# ==============================================================================

import argparse
import dask
import geocat.comp as gc
import logging
import metpy
import numpy  as np
import os
import socket
import sys
import time
import datetime
import xarray as xr 
import warnings

# ==============================================================================
# ==============================================================================
# CUSTOM USER FUNCTIONS
# ==============================================================================
# ==============================================================================

# ==============================================================================
# FUNCTION: custom variable list
# 
# Pass the list of variables for import 
# ==============================================================================


def custom_variable_list():
    
    logging.info("Getting list of variables for import")
    
    # Variables to be read-in
    netcdf_variables = [
        "Q",  # in kg/kg
        "T",  # in K
        "V",  # in m/s
        "Z3", # in m
        "PS", # need for interp from hybrid to pressure levels
        
        # Add more variables here
    ]
    
    for var in netcdf_variables:
        logging.debug(f" * {var}")
    
    return netcdf_variables



# ==============================================================================
# FUNCTION: custom preprocess function
# 
# Perform the data preprocessing for a single ensemble member
# ==============================================================================

def custom_preprocess_function(
    dset_ens,
    case_name,
    preprocess_kwargs,
    parallel="TRUE"):
    
    preprocess_debugging_text = f'''    
================================================================================
Debugging text for ensemble member: {case_name}'
================================================================================

{dset_ens}

-------------------------------------CHUNKS-------------------------------------
{dset_ens.chunks}
--------------------------------------------------------------------------------
    '''
    
    logging.debug(preprocess_debugging_text)
    
    # Create a new dataset to hold data after preprocessing has occurred
    dset_ens_preprocessed = xr.Dataset()
    
    # INSERT PREPROCESS CODE HERE
    #
    # THE INPUT "dset_ens" INCLUDES DATA FOR A SINGLE ENSEMBLE MEMBER.
    #
    # THE OUTPUT "dset_ens_preprocessed" INCLUDES DATA FOR A SINGLE ENSEMBLE
    # MEMBER, WHICH HAS BEEN PREPROCESSED. EVEN IF ONLY ONE VARIABLE IS USED, IT
    # IS RECOMMENDED TO PASS THE VARIABLE WITHIN A DATASET INSTEAD OF WITHIN A 
    # DATAARRAY, OTHERWISE CHANGES MAY NEED TO BE MADE ELSEWHERE.
    
    return dset_ens_preprocessed # output should be an xr dataset

# ==============================================================================
# FUNCTION: custom analysis function
# 
# Perform the primary data analysis for a single ensemble member
# ==============================================================================

def custom_anaylsis_function(
    dset_ens_preprocessed,
    case_name,
    preprocess_kwargs,
    parallel="TRUE"):
    
    logging.debug(f'Performing data analysis for ensemble member: {case_name}')
    logging.debug(dset_ens_preprocessed)
    
    # Create a new dataset to hold data after analysis has occurred
    dset_ens_analyzed = xr.Dataset()
    
    # INSERT ANALYSIS CODE HERE
    #
    # THE INPUT "dset_ens_preprocessed" INCLUDES DATA FOR A SINGLE ENSEMBLE MEMBER.
    #
    # THE OUTPUT "dset_ens_preprocessed" INCLUDES DATA FOR A SINGLE ENSEMBLE
    # MEMBER, WHICH HAS BEEN PREPROCESSED. EVEN IF ONLY ONE VARIABLE IS USED, IT
    # IS RECOMMENDED TO PASS THE VARIABLE WITHIN A DATASET INSTEAD OF WITHIN A 
    # DATAARRAY, OTHERWISE CHANGES MAY NEED TO BE MADE ELSEWHERE.    
    
    if parallel == "FALSE":
        logging.debug("PARALLEL=FALSE: Computing analyzed data...")
        dset_ens_analyzed = dset_ens_analyzed.compute()
    
    return dset_ens_analyzed # output should be an xr dataset

# ==============================================================================
# FUNCTION: custom combination function
# 
# Combine output from every ensemble member into a desired format
# ==============================================================================


def concat_and_save(file_list,casenames,save_file,parallel):
    
    if parallel == "TRUE":
        parallel_or_serial_open_function = dask.delayed(xr.open_dataset)
        
    else:
        parallel_or_serial_open_function = xr.open_dataset
    
    logging.info(f'Combining data from all ensemble members...')
    
    # Empty dict to hold xr objects prior to concat
    dset_dict = {}
    
    # Iterate over files
    for file,case in zip(file_list,casenames):
        logging.debug(f'Reading data from case: {case}')
        dset_dict[case] = parallel_or_serial_open_function(file)
        
    dsets_computed = dask.compute(*dset_dict.values())
        
    # Combine all of the analysis output into a single xr dataset along a new
    # dimension of "ensemble member"
    dset_concat = xr.concat([x for x in dsets_computed],dim='ensemble_member')
    
    # Give appropriate names to the ensemble members, rather than just indexes
    dset_concat = dset_concat.assign_coords({'ensemble_member':casenames})        
    
    logging.info(f'Saving to a single file...')
    
    if parallel == "TRUE":
    
        dset_concat = dset_concat.to_netcdf(save_file,compute=False)
        
        dset_concat.compute()
        
    else:
        
        dset_concat.to_netcdf(save_file,compute=True)
        
    logging.info(f'Data saved to {save_file}')
    
    return      

        
def generate_save_filename(save_subdir,save_name,ens_name,nc_file_timestr,ens_member="",combined=False,n_ens=""):
    
    if combined:
        
        ens_filename = save_subdir + f"{ens_name}_{save_name}_{n_ens}members_{nc_file_timestr}.nc"
        
    else:
        
        ens_filename = save_subdir + f"{ens_name}_{ens_member}_{save_name}_{nc_file_timestr}.nc"        
    
    return ens_filename
        
        
def save_single_ensemble_member(dset_save,ens_member_filename,parallel):  
    
    logging.debug(f"Saving the following dataset:\n{dset_save}")
    
    if parallel == "TRUE":
    
        dset_save = dask.delayed(dset_save.to_netcdf)(ens_member_filename,compute=True)
        
        dset_save.compute()
        
    else:
        
        dset_save = dset_save.to_netcdf(ens_member_filename)
    
    return dset_save
    
