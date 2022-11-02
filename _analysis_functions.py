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

from dask.array.core import map_blocks
from dask.distributed import Client
from dask_jobqueue import PBSCluster
from ncar_jobqueue import NCARCluster

from metpy.interpolate import interpolate_1d

# Suppress a metpy interpolation warning
warnings.filterwarnings(
    "ignore", 
    message="Interpolation point out of data bounds encountered"
)

# ==============================================================================
# VARIABLES for functions in this script
# ==============================================================================

c_p         = 1.00464e3            # specific heat of dry air ~ J/kg/K
g           = 9.80616              # acceleration due to gravity, earth
L_v         = 2.501e6              # latent heat of evaporation ~ J/kg

# Pressure levels for interpolation (write in hPa then convert to Pa)
new_levels = np.array([
    1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100
]).astype(np.float32) * 100

func_interpolate = interpolate_1d

# ==============================================================================
# FUNCTION: Parse command line arguments
# ==============================================================================

def parse_command_line_arguments():
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--casenames_file',type=str)
    parser.add_argument('--concat_results',type=str)
    parser.add_argument('--data_freq',type=str)
    parser.add_argument('--data_level',type=str,default="-1.0")
    parser.add_argument('--ensemble_name',type=str)
    parser.add_argument('--job_scheduler',type=str)
    parser.add_argument('--parallel',type=str,default="TRUE")
    parser.add_argument('--nc_file_timestr',type=str,default="NONE")
    parser.add_argument('--save_path',type=str)
    parser.add_argument('--save_name',type=str)
    parser.add_argument('--skip_analysis',type=str,default="FALSE")    
    parser.add_argument('--testing_mode',type=str,default="FALSE")
    parser.add_argument('--testing_mode_short',type=str,default="FALSE")    
    parser.add_argument('--user',type=str)
    parser.add_argument('--verbose', nargs='?', type=int, const=10, default=20)

    args      = parser.parse_args()
    
    return args

# ==============================================================================
# FUNCTION: Get supported ensembles
# ==============================================================================

def get_supported_ensembles():
    supported_ensembles = [
        "CESM2-SF",
        "CESM2-LE",
    ]
    
    return supported_ensembles


# ==============================================================================
# FUNCTION: Get ensemble data path
# ==============================================================================

def get_ensemble_data_path(ensemble_name):

    ensemble_paths = {
        "CESM2-SF":"/glade/campaign/cesm/collections/CESM2-SF/timeseries/atm/proc/tseries/",
        "CESM2-LE":"/glade/campaign/cgd/cesm/CESM2-LE/timeseries/atm/proc/tseries/",
    }
    
    return ensemble_paths[ensemble_name]

# ==============================================================================
# FUNCTION: Get ensemble data path
# ==============================================================================

def get_merge_compat(data_freq):

    data_freq_merge_compat = {
        "month_1":"no_conflicts",
        "day_1":"override",
    }
    
    return data_freq_merge_compat[data_freq]


# ==============================================================================
# FUNCTION: Read Casenames from Text File
# ==============================================================================

def read_casenames(casenames_file):
    
    logging.info("Reading in Case Names")
    
    casenames = []
    with open(casenames_file,mode='r') as file:
        for line in file.readlines():
            casenames.append(line[:-1])
            
    return casenames

# ==============================================================================
# FUNCTION: Generate filenames for each ensemble member
# ==============================================================================

def generate_ensemble_filenames(netcdf_variables,casenames,path,nc_file_timestr,delimeter="&&"):
    
    logging.info("Generating lists of files for each ensemble member")
    
    # Empty dict to hold all files for all ensemble members    
    case_files = {} 

    # Only used for debugging
    ncases = len(casenames)
    i = 1

    # Generate a list of filenames for each ensemble member
    for CASENAME in casenames:
        
        logging.debug(f"Generating Files for Case {i} of {ncases}: {CASENAME}")

        # Initialize an empty list
        CASEFILES_LIST = []    
        
        if delimeter in CASENAME:
            CASENAME_LIST = CASENAME.split(delimeter)
            
        else:
            CASENAME_LIST = [CASENAME]

        # Iterate over desired variables: this is because data is stored in 
        # individual timeseries files for each variable
        for var in netcdf_variables:
            
            # Temporary file directory for a given variable. Note that the file directory
            # holds files for every ensemble member
            file_dir_tmp = path + var + "/"

            # Get all files for a given variable
            files_tmp = [file_dir_tmp + x for x in os.listdir(file_dir_tmp) if ".nc" in x]
            
            # -----------------------------------------------------------------
            # START: Treatment for same-case, different-name 
            # (e.g., a single run with historical forcing and then a SSP
            # forcing, where the historical / ssp data have slightly different
            # filenames that prevent simple string comprehension)
            # -----------------------------------------------------------------            
            files_tmp_case = []
            
            for CASE in CASENAME_LIST:
                
                # Filter to only include files for the specified ensemble member
                files_tmp_case = files_tmp_case + [str(x) for x in files_tmp if CASE in x]
                        
            # Add files for the given variable to list of all files for the specified
            # ensemble member
            CASEFILES_LIST = np.sort(np.append(CASEFILES_LIST,files_tmp_case))
            
        # Store the list of files for a specific ensemble member in the dict
        case_files[CASENAME] = CASEFILES_LIST
        
        i += 1
        
    # Optional additional filter of nc files based on time
    if nc_file_timestr != "NONE":
        for key in case_files.keys():
            case_files[key] = np.array([x for x in case_files[key] if nc_file_timestr in x])
            
    return case_files

def setup_cluster(user,job_scheduler="PBS"):
    
    # For a list of interfaces, run $ ifconfig 
    interface = 'lo'
    
    ncores = 4
    nprocesses = 12
    memory = '256GB'
    
    if job_scheduler == "PBS":
        
        logging.info("Setting up a dask PBSCluster")
    
        # Setup your PBSCluster - make sure that it uses the casper queue
        cluster = PBSCluster(
                cores=2, # The number of cores you want
                memory='256GB', # Amount of memory
                processes=12, # How many processes
                queue='casper', # The type of queue to utilize (/glade/u/apps/dav/opt/usr/bin/execcasper)
                local_directory='$TMPDIR', # Use your local directory
                resource_spec='select=1:ncpus=6:mem=128GB', # Specify resources
                project='UHAR0008', # Input your project ID here
                walltime='12:00:00', # Amount of wall time
                interface=interface, # Interface to use
            )
        
    elif job_scheduler == "NCAR":
        
        logging.info("Setting up a dask PBSCluster on an NCAR machine using ncar-jobqueue")
        
        cluster = NCARCluster(
                cores=4, # The number of cores you want
                memory='256GB', # Amount of memory
                processes=4, # How many processes
                queue='casper', # The type of queue to utilize (/glade/u/apps/dav/opt/usr/bin/execcasper)
                local_directory='$TMPDIR', # Use your local directory
                resource_spec='select=1:ncpus=6:mem=128GB', # Specify resources
                project='UHAR0008', # Input your project ID here
                walltime='12:00:00', # Amount of wall time
                interface='ib0', # Interface to use
            )
        
    # elif job_scheduler == "SLURM":
        
        # logging.info("Setting up a dask SLURMCluster)
        # use dask-jobqueue.SLURMCluster to set up a cluster       
        
    else:
        
        cluster = "FAILED"
        client  = "FAILED"
        
        job_scheduler_not_recognized_message = f'''
=======================================================================================================================
!!!UNABLE TO SETUP DASK CLUSTER!!!
=======================================================================================================================
THE JOB_SCHEDULER SUPPLIED BY THE USER: 
        {job_scheduler}
        
IS NOT CURRENTLY SUPPORTED BY THIS APPLICATION.
THIS CAN BE REMIDIED BY ADDING AN OPTION TO 
        setup_cluster() IN THE SCRIPT _analysis_functions.py
        
EXITING
        '''
        
        logging.error(job_scheduler_not_recognized_message)
        
        return cluster, client 
        
    cluster.scale(10)

    client = Client(cluster)

    # This is just to forward the host for ssh tunnelling so the dask dashboard
    # can be viewed remotely.
    host = client.run_on_scheduler(socket.gethostname)
   
        
    dask_logging_text = f'''    
=======================================================================================================================
DASK DIAGNOSTICS
=======================================================================================================================
To view the dask diagnostic dashboard for this job, follow this URL for a local or login-node job:
        {client.dashboard_link}
        
If this is a remote PBS job on a HPC, need to first use SSH tunnelling. Login to cheyenne with the following command:
        ssh -N -L 8888:{host}:8888  -L 8787:{host}:8787 {user}@casper.ucar.edu
Then use this url:
        http://localhost:8787/status
        
        '''

    logging.info(dask_logging_text)           
    
    return cluster, client