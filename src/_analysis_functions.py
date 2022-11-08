'''Functions used by _ensemble_analysis

Author: Michael R. Needham (m.needham@colostate.edu)
Last Updated: 2 November 2022

These functions should likely not be edited by the user, with the possible
exception of:
    * get_supported_ensembles()
    * get_ensemble_data_path()
    * parse_command_line_arguments()
'''

# ==============================================================================
# Import Statements
# ==============================================================================

import logging
import os
import socket

import argparse

import dask
import xarray as xr

import numpy as np

from dask.distributed import Client
from dask_jobqueue import PBSCluster
from ncar_jobqueue import NCARCluster

# ==============================================================================
# FUNCTION: Parse command line arguments
# ==============================================================================


def parse_command_line_arguments():
    '''Simple function to read command line arguements

    If necessary, the user can add arguments. If this is done, the corresponding
    variable should be included in submit.sh in the line:

        python3 _ensemble_analysis.py ...
    '''

    parser = argparse.ArgumentParser()

    parser.add_argument('--casenames_file', type=str)
    parser.add_argument('--concat_results', type=str)
    parser.add_argument('--data_freq', type=str)
    parser.add_argument('--ensemble_name', type=str)
    parser.add_argument('--job_scheduler', type=str)
    parser.add_argument('--parallel', type=str, default="TRUE")
    parser.add_argument('--preprocess_kwargs', type=str, default="TRUE")
    parser.add_argument('--nc_file_timestr', type=str, default="NONE")
    parser.add_argument('--save_path', type=str)
    parser.add_argument('--save_name', type=str)
    parser.add_argument('--skip_analysis', type=str, default="FALSE")
    parser.add_argument('--skip_preprocess', type=str, default="TRUE")
    parser.add_argument('--testing_mode_n_ens', type=str, default="FALSE")
    parser.add_argument('--testing_mode_n_time', type=str, default="FALSE")
    parser.add_argument('--user', type=str)
    parser.add_argument('--user_file_path', type=str)
    parser.add_argument('--verbose', nargs='?', type=int, const=10, default=20)

    args = parser.parse_args()

    return args

# ==============================================================================
# FUNCTION: Get supported ensembles
# ==============================================================================


def get_supported_ensembles():
    '''List of ensembles supported by the program'''

    supported_ensembles = [
        "CESM2-LE",
        "CESM2-SF",
        "CESM1-LE",
        "CESM1-SF",
    ]

    return supported_ensembles


# ==============================================================================
# FUNCTION: Get ensemble data path
# ==============================================================================

def get_ensemble_data_path(ensemble_name):
    '''Path to data for ensembles supported by the program'''

    ensemble_paths = {
        "CESM2-SF": "/glade/campaign/cesm/collections/CESM2-SF/timeseries/atm/proc/tseries/",
        "CESM2-LE": "/glade/campaign/cgd/cesm/CESM2-LE/timeseries/atm/proc/tseries/",
        "CESM1-LE": "/glade/campaign/cesm/collections/cesmLE/CESM-CAM5-BGC-LE/atm/proc/tseries/",
        "CESM1-SF": "/glade/campaign/cesm/collections/cesmLE/CESM-CAM5-BGC-LE/atm/proc/tseries/"}

    return ensemble_paths[ensemble_name]

# ==============================================================================
# FUNCTION: Get ensemble data path
# ==============================================================================


def get_merge_compat(data_freq):
    '''String to pass to xr.open_mfdataset depending on data frequency'''

    data_freq_merge_compat = {
        "month_1": "no_conflicts",
        "day_1": "override",
    }

    return data_freq_merge_compat[data_freq]


# ==============================================================================
# FUNCTION: Read Casenames from Text File
# ==============================================================================

def read_casenames(casenames_file):
    '''Function to read casenames from casenames.txt file generated by _generate_casenames.py'''

    logging.info("Reading in Case Names")

    casenames = []
    with open(casenames_file, mode='r', encoding="utf-8") as file:
        for line in file.readlines():
            casenames.append(line[:-1])

    return casenames

# ==============================================================================
# FUNCTION: Generate filenames for each ensemble member
# ==============================================================================


def generate_ensemble_filenames(
        netcdf_variables,
        casenames,
        path,
        nc_file_timestr,
        delimeter="&&"):
    '''Function to generate lists of files for import'''

    logging.info("Generating lists of files for each ensemble member")

    # Empty dict to hold all files for all ensemble members
    case_files = {}

    # Only used for debugging
    ncases = len(casenames)
    i = 1

    # Generate a list of filenames for each ensemble member
    for casename in casenames:

        logging.debug(
            "Generating Files for Case %s of %s: %s",
            i,
            ncases,
            casename)

        # Initialize an empty list
        casefiles_list = []

        if delimeter in casename:
            casename_list = casename.split(delimeter)

        else:
            casename_list = [casename]

        # Iterate over desired variables: this is because data is stored in
        # individual timeseries files for each variable
        for var in netcdf_variables:

            # Temporary file directory for a given variable. Note that the file directory
            # holds files for every ensemble member
            file_dir_tmp = path + var + "/"

            # Get all files for a given variable
            files_tmp = [
                file_dir_tmp +
                x for x in os.listdir(file_dir_tmp) if ".nc" in x]

            logging.debug(
                "Total Number of Files for Variable %s, %s",
                var,
                len(files_tmp)
            )

            # -----------------------------------------------------------------
            # START: Treatment for same-case, different-name
            # (e.g., a single run with historical forcing and then a SSP
            # forcing, where the historical / ssp data have slightly different
            # filenames that prevent simple string comprehension)
            # -----------------------------------------------------------------
            files_tmp_case = []

            for case in casename_list:

                # Filter to only include files for the specified ensemble
                # member
                files_tmp_case = files_tmp_case + \
                    [str(x) for x in files_tmp if case in x]

            # Add files for the given variable to list of all files for the specified
            # ensemble member
            casefiles_list = np.sort(np.append(casefiles_list, files_tmp_case))

        # Store the list of files for a specific ensemble member in the dict
        case_files[casename] = casefiles_list

        i += 1

    # Optional additional filter of nc files based on time
    if nc_file_timestr != "NONE":
        for key in case_files.copy():
            case_files[key] = np.array(
                [x for x in case_files[key] if nc_file_timestr in x])

    return case_files


def setup_cluster(user, job_scheduler="PBS", ncores=4, nprocesses=12,
                  memory='256GB', interface='lo'):
    '''Function to setup the dask cluster and interact with appropriate job scheduler'''

    # For a list of interfaces, run $ ifconfig

    if job_scheduler == "PBS":

        logging.info("Setting up a dask PBSCluster")

        # Setup your PBSCluster - make sure that it uses the casper queue
        cluster = PBSCluster(
            cores=ncores,  # The number of cores you want
            memory=memory,  # Amount of memory
            processes=nprocesses,  # How many processes
            queue='casper',
            local_directory='$TMPDIR',  # Use your local directory
            resource_spec='select=1:ncpus=6:mem=128GB',  # Specify resources
            project='UHAR0008',  # Input your project ID here
            walltime='12:00:00',  # Amount of wall time
            interface=interface,  # Interface to use
        )

    elif job_scheduler == "NCAR":

        logging.info(
            "Setting up a dask PBSCluster on an NCAR machine using ncar-jobqueue")

        cluster = NCARCluster(
            cores=ncores,  # The number of cores you want
            memory=memory,  # Amount of memory
            processes=nprocesses,  # How many processes
            queue='casper',
            # The type of queue to utilize
            # (/glade/u/apps/dav/opt/usr/bin/execcasper)
            local_directory='$TMPDIR',  # Use your local directory
            resource_spec='select=1:ncpus=6:mem=128GB',  # Specify resources
            project='UHAR0008',  # Input your project ID here
            walltime='12:00:00',  # Amount of wall time
            interface='ib0',  # Interface to use
        )

    # elif job_scheduler == "SLURM":

        # logging.info("Setting up a dask SLURMCluster)
        # use dask-jobqueue.SLURMCluster to set up a cluster

    else:

        cluster = "FAILED"
        client = "FAILED"

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

# ==============================================================================
# FUNCTION: custom combination function
#
# Combine output from every ensemble member into a desired format
# ==============================================================================


def concat_and_save(file_list, casenames, save_file, parallel):
    '''Function to concatenate all ensemble members into a single dataset prior to saving'''

    if parallel == "TRUE":
        parallel_or_serial_open_function = dask.delayed(xr.open_dataset)

    else:
        parallel_or_serial_open_function = xr.open_dataset

    logging.info('Combining data from all ensemble members...')

    # Empty dict to hold xr objects prior to concat
    dset_dict = {}

    # Iterate over files
    for file, case in zip(file_list, casenames):
        logging.debug('Reading data from case: %s', case)
        dset_dict[case] = parallel_or_serial_open_function(file)

    dsets_computed = dask.compute(*dset_dict.values())

    # Combine all of the analysis output into a single xr dataset along a new
    # dimension of "ensemble member"
    dset_concat = xr.concat(list(dsets_computed), dim='ensemble_member')

    # Give appropriate names to the ensemble members, rather than just indexes
    dset_concat = dset_concat.assign_coords({'ensemble_member': casenames})

    logging.info('Saving to a single file...')

    if parallel == "TRUE":

        dset_concat = dset_concat.to_netcdf(save_file, compute=False)

        dset_concat.compute()

    else:

        dset_concat.to_netcdf(save_file, compute=True)

    logging.info('Data saved to %s', save_file)


def generate_save_filename(
        save_subdir,
        save_name,
        ens_name,
        nc_file_timestr,
        ens_member="",
        combined=False,
        n_ens="",
        testing_mode_n_time="FALSE"
):
    '''Function to generate a unique filename for each ensemble member'''

    if nc_file_timestr == "NONE":
        nc_file_timestr = ""

    if combined:

        ens_filename = save_subdir + \
            f"{ens_name}_{save_name}_{n_ens}members_{nc_file_timestr}"

    else:

        ens_filename = save_subdir + \
            f"{ens_name}_{ens_member}_{save_name}_{nc_file_timestr}"

    if testing_mode_n_time == "TRUE":

        logging.debug("Appending \"_TESTINGMODE\" to Filename")

        ens_filename = ens_filename + "_TESTINGMODE"

    else:
        logging.debug("Not Appending to Filename")

    return ens_filename + ".nc"


def save_single_ensemble_member(dset_save, ens_member_filename, parallel):
    '''Function to save a netcdf file for a single ensemble member'''

    logging.debug("Saving the following dataset:\n%s", dset_save)

    if parallel == "TRUE":

        dset_save = dask.delayed(
            dset_save.to_netcdf)(
            ens_member_filename,
            compute=True)

        dset_save.compute()

    else:

        dset_save = dset_save.to_netcdf(ens_member_filename)

    return dset_save
