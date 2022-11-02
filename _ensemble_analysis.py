# ==============================================================================
# Import Statements
# ==============================================================================

import argparse
import dask
import geocat.comp as gc
import logging
import numpy  as np
import os
import socket
import sys
import time
import datetime
import xarray as xr 

from dask.array.core import map_blocks
from dask.distributed import Client
from dask_jobqueue import PBSCluster

from _analysis_functions import *

# ==============================================================================
# Main Function Call
# ==============================================================================
def main():
    
    # ==========================================================================
    # Section 1
    # ==========================================================================
    #    * 1.A Parse command line argument
    #    * 1.B Initialize Logging
    #    * 1.C Setup Parallel / Serial Analysis
    #    * 1.D Read in list of case names from file
    # ==========================================================================
    
    start_time = datetime.datetime.now()
    
    # --------------------------------------------------------------------------
    # 1.A Parse command line argument
    # -------------------------------------------------------------------------- 
    
    args            = parse_command_line_arguments()

    CASENAMES_FILE     = args.casenames_file
    CONCAT_RESULTS     = args.concat_results.upper()
    DATA_FREQ          = args.data_freq
    DATA_LEVEL         = float(args.data_level)
    ENSEMBLE_NAME      = args.ensemble_name.upper()
    JOB_SCHEDULER      = args.job_scheduler.upper()
    NC_FILE_TIMESTR    = args.nc_file_timestr.upper()
    PARALLEL           = args.parallel.upper()
    SAVE_PATH          = args.save_path
    SAVE_NAME          = args.save_name
    SKIP_ANALYSIS      = args.skip_analysis.upper()
    TESTING_MODE       = args.testing_mode.upper()
    TESTING_MODE_SHORT = args.testing_mode_short.upper()    
    VERBOSE            = args.verbose 
    USER               = args.user  
    
    # --------------------------------------------------------------------------
    # 1.B Initialize Logging
    # --------------------------------------------------------------------------    

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        encoding='utf-8',
        level=VERBOSE, # default level is 20 (info)
        datefmt='%Y-%m-%d %H:%M:%S',        
    )

    logging.info(f'Logging initialized at level {VERBOSE}.')
    
    if SKIP_ANALYSIS == "TRUE":
        
        skip_analysis_text = f'''

======================================================================================================================
SCRIPT RUN TO GENERATE CASENAMES ONLY
ANALYSIS WILL NOT BE PERFORMED

TO PERFORM ANALYSIS, SET SKIP_ANALYSIS=\"FALSE\"
EXITING.
======================================================================================================================
            '''

        logging.warning(skip_analysis_text)
        
        return
    
    #def parallel_or_serial_open # <--- REMOVE if script works
    # --------------------------------------------------------------------------
    # 1.C Setup Parallel / Serial Analysis
    # --------------------------------------------------------------------------
    if PARALLEL == "FALSE":
        
        logging.info(f"Flag \"parallel\" set to FALSE. Computation Proceeding in Serial")
        
        parallel_or_serial_open_function       = xr.open_mfdataset
        parallel_or_serial_preprocess_function = custom_preprocess_function
        parallel_or_serial_analysis_function   = custom_anaylsis_function
        parallel_or_serial_save_function       = save_single_ensemble_member        
    
    elif PARALLEL == "TRUE":
        
        logging.info(f"Flag \"parallel\" set to TRUE.")
        
        parallel_or_serial_open_function       = dask.delayed(xr.open_mfdataset)
        parallel_or_serial_preprocess_function = dask.delayed(custom_preprocess_function)
        parallel_or_serial_analysis_function   = dask.delayed(custom_anaylsis_function)
        parallel_or_serial_save_function       = dask.delayed(save_single_ensemble_member)
        
        logging.info(f'Initializing dask client')
        
        cluster, client = setup_cluster(user=USER,job_scheduler=JOB_SCHEDULER)
        
        if type(cluster) == str:
            return
        
    else:
        
        logging.error(f"UNABLE TO INTERPRET FLAG PARALLEL = \"{args.parallel}\"")
        logging.error(f"PARALLEL MUST BE EITHER \"TRUE\" OR \"FALSE\"")
        logging.error(f"EXITING")
        
        return

    # --------------------------------------------------------------------------
    # 1.D Read in list of case names from file
    # -------------------------------------------------------------------------- 
            
    CASENAMES = read_casenames(casenames_file=CASENAMES_FILE)
    
    # By default do not perform any subsetting in time, but if 
    # TESTING_MODE_SHORT = "TRUE", will use this variable to only select the
    # first ten timesteps
    isel_time = dict(time=slice(None,None))
    
    if TESTING_MODE == "TRUE":
        
        n_ensembles_for_test = 2
    
        testing_logging_text = f'''

======================================================================================================================
RUNNING SCRIPT IN TESTING MODE
======================================================================================================================
ANALYZING {n_ensembles_for_test} ENSEMBLE MEMBERS
            '''

        logging.warning(testing_logging_text)

        CASENAMES = CASENAMES[:n_ensembles_for_test]
        
        
        if TESTING_MODE_SHORT == "TRUE":
            
            logging.warning("FLAG \"TESTING_MODE_SHORT\" SET TO \"TRUE\"")
            logging.warning("ONLY ANALZING FIRST 10 TIMESTEPS")            
            
            isel_time = dict(time=slice(1,10))
            
        else:
            
            # don't need to do anything
            pass
        
    elif TESTING_MODE == "FALSE":
        
        # don't need to do anything
        pass
    
    # Catch unrecognized argument for TESTING_MODE flag
    else: 
        
        logging.error(f"UNABLE TO INTERPRET FLAG TESTING_MODE = \"{args.testing_mode}\"")
        logging.error(f"TESTING_MODE MUST BE EITHER \"TRUE\" OR \"FALSE\"")
        logging.error(f"EXITING")
        
    ncases = len(CASENAMES)
    
    # ==========================================================================
    # Section 1 - COMPLETE
    # ==========================================================================
    
    # ==========================================================================
    # Section 2
    # ==========================================================================
    #    * 2.A Generate list of filenames for each ensemble member
    #    * 2.B Iterate over ensemble members
    #        * 2.B.1 Data preprocessing
    #        * 2.B.2 Primary analysis
    #        * 2.B.C Save results
    #    * 2.C
    #       * PARALLEL: Perform delayed computation
    #       * SERIAL: No Action
    #    * 2.D Optional: Concat results and save to disk
    # ==========================================================================    

    # --------------------------------------------------------------------------
    # 2.A Generate list of filenames for each ensemble member
    # --------------------------------------------------------------------------  

    # Get list of variables to load
    NETCDF_VARIABLES = custom_variable_list()
    
    DATA_PATH = get_ensemble_data_path(ENSEMBLE_NAME) + DATA_FREQ + "/"
    
    CASE_FILES = generate_ensemble_filenames(
        netcdf_variables = NETCDF_VARIABLES,
        casenames        = CASENAMES,
        path             = DATA_PATH,
        nc_file_timestr  = NC_FILE_TIMESTR
    )  
    
    MERGE_COMPAT = get_merge_compat(DATA_FREQ)
    
    # Printout of analysis level for user
    netcdf_variable_logging_text = f'''
    
======================================================================================================================    
Analysis will be performed at {DATA_LEVEL} hPa
======================================================================================================================
    '''
    
    logging.info(netcdf_variable_logging_text)
    logging.info(f"Netcdf Variablels:")
    for VAR in NETCDF_VARIABLES:
        logging.info(f"* {VAR}")    
    
    # --------------------------------------------------------------------------
    # 2.B Prepare directory for saving output
    # --------------------------------------------------------------------------
    
    NEW_SAVE_DIRECTORY    = SAVE_PATH + ENSEMBLE_NAME + "/"
    NEW_SAVE_SUBDIRECTORY = NEW_SAVE_DIRECTORY + "INDIVIDUAL-FILES/"
    
    # Make the save path if it does not yet exist
    if not os.path.exists(SAVE_PATH):
        os.mkdir(SAVE_PATH)        
    
    # Make the directory if it does not yet exist
    if not os.path.exists(NEW_SAVE_DIRECTORY):
        os.mkdir(NEW_SAVE_DIRECTORY)    
    
    # Make the subdirectory if it does not yet exist
    if not os.path.exists(NEW_SAVE_SUBDIRECTORY):
        os.mkdir(NEW_SAVE_SUBDIRECTORY)
    
    # --------------------------------------------------------------------------
    # 2.C Iterate over ensemble members
    # --------------------------------------------------------------------------  
    
    logging.info(f'Iterating over ensemble members:')
    
    # Empty dict to hold analysis output for every ensemble member
    ANALYSIS_OUTPUT_LIST = {}
    
    # Dummy variable to use in del statement, below, during the first ens member 
    dset_ens = xr.Dataset()
    
    # 
    icase = 1
    
    for ENS_MEMBER in CASENAMES:
        
        # --------------------------------------------------------------------------
        # 2.B.1 Prepare Parallel or Serial Analysis
        # --------------------------------------------------------------------------
        
        if PARALLEL == "TRUE":
        
            logging.debug(f' * Prepare task graph for Case {icase} of {ncases}: {ENS_MEMBER}')
            
        else:
            
            # Relese memory from the previous ensemble member
            del dset_ens
            
            logging.info(f'* Analyzing data for Case {icase} of {ncases}: {ENS_MEMBER}')

        
        # Get the files for the particular ensemble member
        ens_member_files = CASE_FILES[ENS_MEMBER]
        
        # Need to specify as 9 or below to log all files
        if int(VERBOSE) < 10:
            logging.debug("Printing filenames..")
            for file in ens_member_files:
                logging.debug(f"* {file}")
                
        # read the data - use dask delayed
        # If testing_mode_short == TRUE, only select 10 timesteps
        dset_ens = parallel_or_serial_open_function(
            ens_member_files, 
            combine='by_coords',
            compat=MERGE_COMPAT,
            parallel=PARALLEL,
            chunks={'time':100,'lat':192,'lon':288},
        ).isel(isel_time)
        
        # Preprocess the data
        dset_ens = parallel_or_serial_preprocess_function(dset_ens,ENS_MEMBER,DATA_LEVEL)
    
        # Create the task graph of the custom analysis function for lazy eval
        dset_ens = parallel_or_serial_analysis_function(
            dset_ens_preprocessed=dset_ens,
            case_name=ENS_MEMBER,
            data_level=DATA_LEVEL,
            parallel=PARALLEL,
        )
        
        # not parallelized, simple string manipulation
        ENS_MEMBER_FILENAME = generate_save_filename(
            save_subdir=NEW_SAVE_SUBDIRECTORY,
            save_name=SAVE_NAME,
            ens_name=ENSEMBLE_NAME,
            ens_member=ENS_MEMBER,
            nc_file_timestr=NC_FILE_TIMESTR
        )
               
        # SAVE the data to disk
        dset_ens = parallel_or_serial_save_function(
            dset_save=dset_ens,
            ens_member_filename = ENS_MEMBER_FILENAME,
            parallel=PARALLEL
        )
        
        # --------------------------------------------------------------------------
        # 2.B.2 Collect results (delayed objects)
        # --------------------------------------------------------------------------  
        
        # Store the delayed objects in a dict for later computation. Use filenames for keys
        ANALYSIS_OUTPUT_LIST[ENS_MEMBER_FILENAME] = dset_ens
        
        icase += 1
        
    logging.info('COMPLETED Iterating over ensemble members.')
        
    # --------------------------------------------------------------------------
    # 2.D.PARALLEL Perform delayed computation and collect results
    # --------------------------------------------------------------------------
    
    if PARALLEL == "TRUE":
           
        logging.info(f'Performing delayed parallel computation.\nNote, the wait here may also indicate that the PBS job is waiting in the job queue.')

        ANALYSIS_OUTPUT_COMPUTED_LIST = dask.compute([x for x in ANALYSIS_OUTPUT_LIST.values()])[0]
        
    # --------------------------------------------------------------------------
    # 2.D.SERIAL Collect Results
    # --------------------------------------------------------------------------        
        
    else:
        
        # Take the values of the computation as a list
        ANALYSIS_OUTPUT_COMPUTED_LIST = [x for x in ANALYSIS_OUTPUT_LIST.values()]
        
    # --------------------------------------------------------------------------
    # 2.E CONCAT RESULTS
    # --------------------------------------------------------------------------             
    
    logging.info(f'Individual files saved to {NEW_SAVE_SUBDIRECTORY}')
    filenames = [x for x in ANALYSIS_OUTPUT_LIST.keys()]
    
    if VERBOSE <= 10:
        for file in filenames:
            logging.debug(f"* {file}")
                              
    if CONCAT_RESULTS == "TRUE":
        
        logging.info("Concatenating Files...")
        
        combined_save_name = generate_save_filename(
            save_subdir=NEW_SAVE_DIRECTORY,
            save_name=SAVE_NAME,
            ens_name=ENSEMBLE_NAME,
            nc_file_timestr=NC_FILE_TIMESTR,
            combined=True,
            n_ens=len(filenames)
        )
        
        concat_and_save(
            file_list=filenames,
            save_file=combined_save_name,
            casenames=CASENAMES,
            parallel=PARALLEL
        )
        
    else:
        
        logging.info(f"User Flag \"CONCAT_RESULTS\" set to \"FALSE\"")  
   
    end_time = datetime.datetime.now()
    
    time_delta = end_time - start_time
    
    logging.info('Analysis script complete.')
    
    logging.info(f'Analysis script duration (including PBSCluster Queueing):    {time_delta}')
    
    if PARALLEL == "TRUE":
        
        logging.info("Closing cluster...")
        
        try:
            cluster.close()
            client.shutdown()
    
        # Ignore an error associated with shutting down the cluster
        except AssertionError:
            pass
    
    # ==========================================================================
    # Section 2 - COMPLETE
    # ==========================================================================
        
if __name__ == "__main__":
    main()