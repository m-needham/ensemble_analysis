'''Primary ensemble analysis

Author: Michael R. Needham (m.needham@colostate.edu)
Last Updated: 2 November 2022
'''

# ==============================================================================
# Import Statements
# ==============================================================================

import datetime
import logging
import os

import dask
import xarray as xr

from _analysis_functions import (
    concat_and_save,
    generate_ensemble_filenames,
    generate_save_filename,
    get_ensemble_data_path,
    get_merge_compat,
    parse_command_line_arguments,
    read_casenames,
    save_single_ensemble_member,
    setup_cluster,
)

from _user_functions import (
    custom_anaylsis_function,
    custom_variable_list,
    custom_preprocess_function,
)

from _generate_casenames import data_freq_compatibility

# ==============================================================================
# Main Function Call
# ==============================================================================


def main():
    '''Main analysis function

    PROCEDURE

    Section 1
     * 1.A Parse command line argument
     * 1.B Initialize Logging
     * 1.C Setup Parallel / Serial Analysis
     * 1.D Read in list of case names from file

    Section 2
     * 2.A Generate list of filenames for each ensemble member
     * 2.B Iterate over ensemble members
       * 2.B.1 Data preprocessing
       * 2.B.2 Primary analysis
       * 2.B.C Save results
     * 2.C
       * parallel: Perform delayed computation
       * SERIAL: No Action
     * 2.D Optional: Concat results and save to disk
    '''

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

    args = parse_command_line_arguments()

    casenames_file = args.casenames_file
    concat_results = args.concat_results.upper()
    data_freq = args.data_freq
    ensemble_name = args.ensemble_name.upper()
    job_scheduler = args.job_scheduler.upper()
    nc_file_timestr = args.nc_file_timestr.upper()
    parallel = args.parallel.upper()
    preprocess_kwargs = args.preprocess_kwargs.upper()
    save_path = args.save_path
    save_name = args.save_name
    skip_analysis = args.skip_analysis.upper()
    skip_preprocess = args.skip_preprocess.upper()
    testing_mode_n_ens = args.testing_mode_n_ens.upper()
    testing_mode_n_time = args.testing_mode_n_time.upper()
    verbose = args.verbose
    user = args.user

    # --------------------------------------------------------------------------
    # 1.B Initialize Logging
    # --------------------------------------------------------------------------

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        encoding='utf-8',
        level=verbose,  # default level is 20 (info)
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.info('Logging initialized at level %s', verbose)

    if skip_analysis == "TRUE":

        skip_analysis_text = '''

================================================================================
SCRIPT RUN TO GENERATE casenames ONLY
ANALYSIS WILL NOT BE PERFORMED

TO PERFORM ANALYSIS, SET skip_analysis=\"FALSE\"
EXITING.
================================================================================
            '''

        logging.warning(skip_analysis_text)

        return

    # def parallel_or_serial_open # <--- REMOVE if script works
    # --------------------------------------------------------------------------
    # 1.C Setup Parallel / Serial Analysis
    # --------------------------------------------------------------------------
    if parallel == "FALSE":

        logging.info(
            "Flag \"parallel\" set to FALSE. Computation Proceeding in Serial")

        parallel_or_serial_open_function = xr.open_mfdataset
        parallel_or_serial_preprocess_function = custom_preprocess_function
        parallel_or_serial_analysis_function = custom_anaylsis_function
        parallel_or_serial_save_function = save_single_ensemble_member

    elif parallel == "TRUE":

        logging.info("Flag \"parallel\" set to TRUE.")

        parallel_or_serial_open_function = dask.delayed(xr.open_mfdataset)
        parallel_or_serial_preprocess_function = dask.delayed(
            custom_preprocess_function)
        parallel_or_serial_analysis_function = dask.delayed(
            custom_anaylsis_function)
        parallel_or_serial_save_function = dask.delayed(
            save_single_ensemble_member)

        logging.info('Initializing dask client')

        cluster, client = setup_cluster(user=user, job_scheduler=job_scheduler)

        if isinstance(cluster, str):
            return

    else:

        logging.error(
            "UNABLE TO INTERPRET FLAG parallel = \"%s\"",
            args.parallel)
        logging.error("parallel MUST BE EITHER \"TRUE\" OR \"FALSE\"")
        logging.error("EXITING")

        return

    # --------------------------------------------------------------------------
    # 1.D Read in list of case names from file
    # --------------------------------------------------------------------------

    casenames = read_casenames(casenames_file=casenames_file)

    # By default do not perform any subsetting in time, but if
    # testing_mode_n_time = "TRUE", will use this variable to only select the
    # first ten timesteps
    isel_time = dict(time=slice(None, None))

    if testing_mode_n_ens == "TRUE":

        n_ensembles_for_test = 2

        testing_logging_text = f'''

================================================================================
RUNNING SCRIPT IN TESTING MODE
================================================================================
ANALYZING {n_ensembles_for_test} ENSEMBLE MEMBERS
            '''

        logging.warning(testing_logging_text)

        casenames = casenames[:n_ensembles_for_test]

        if testing_mode_n_time == "TRUE":

            logging.warning("FLAG \"testing_mode_n_time\" SET TO \"TRUE\"")
            logging.warning("ONLY ANALZING FIRST 10 TIMESTEPS")

            isel_time = dict(time=slice(0, 10))

        else:

            logging.info(
                "Flag \"testing_mode_n_time\" set to %s",
                testing_mode_n_time)

    elif testing_mode_n_ens == "FALSE":

        # don't need to do anything
        pass

    # Catch unrecognized argument for testing_mode_n_ens flag
    else:

        logging.error(
            "UNABLE TO INTERPRET FLAG testing_mode_n_ens = \"%s\"",
            args.testing_mode_n_ens)
        logging.error(
            "testing_mode_n_ens MUST BE EITHER \"TRUE\" OR \"FALSE\"")
        logging.error("EXITING")

    ncases = len(casenames)

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
    #       * parallel: Perform delayed computation
    #       * SERIAL: No Action
    #    * 2.D Optional: Concat results and save to disk
    # ==========================================================================

    # --------------------------------------------------------------------------
    # 2.A Generate list of filenames for each ensemble member
    # --------------------------------------------------------------------------

    # Get list of variables to load
    netcdf_variables = custom_variable_list()

    data_freq_str = data_freq_compatibility(ensemble_name,data_freq)
    data_path     = get_ensemble_data_path(ensemble_name) + data_freq_str + "/"

    logging.debug("Inferred Data Path:\n  %s", data_path)

    case_files = generate_ensemble_filenames(
        netcdf_variables=netcdf_variables,
        casenames=casenames,
        path=data_path,
        nc_file_timestr=nc_file_timestr
    )

    merge_compat = get_merge_compat(data_freq)

    logging.info("Netcdf Variables:")
    for var in netcdf_variables:
        logging.info("* %s", var)

    # --------------------------------------------------------------------------
    # 2.B Prepare directory for saving output
    # --------------------------------------------------------------------------

    new_save_directory = save_path + ensemble_name + "/"
    new_save_subdirectory = new_save_directory + "INDIVIDUAL-FILES/"

    # Make the save path if it does not yet exist
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    # Make the directory if it does not yet exist
    if not os.path.exists(new_save_directory):
        os.mkdir(new_save_directory)

    # Make the subdirectory if it does not yet exist
    if not os.path.exists(new_save_subdirectory):
        os.mkdir(new_save_subdirectory)

    # --------------------------------------------------------------------------
    # 2.C Iterate over ensemble members
    # --------------------------------------------------------------------------

    logging.info('Iterating over ensemble members:')

    # Empty dict to hold analysis output for every ensemble member
    analysis_output_list = {}

    # Dummy variable to use in del statement, below, during the first ens
    # member
    dset_ens = xr.Dataset()

    #
    icase = 1

    for ens_member in casenames:

        # ----------------------------------------------------------------------
        # 2.B.1 Prepare Parallel or Serial Analysis
        # ----------------------------------------------------------------------

        if parallel == "TRUE":

            logging.debug(
                ' * Prepare task graph for Case %s of %s: %s',
                icase,
                ncases,
                ens_member)

        else:

            # Relese memory from the previous ensemble member
            del dset_ens

            logging.info(
                '* Analyzing data for Case %s of %s: %s',
                icase,
                ncases,
                ens_member)

        # Get the files for the particular ensemble member
        ens_member_files = case_files[ens_member]

        if len(ens_member_files) == 0:
            logging.warning(
                "NO FILES FOUND FOR THE CURRENT ENSEMBLE MEMBER: %s",
                ensemble_name)
            logging.warning(
                '''
================================================================================
ENSURE COMPATABLE USAGE OF THE FOLLOWING PARAMETERS IN submit.sh:
    * DATA_FREQ
    * ENSEMBLE_NAME
    * NC_FILE_TIMESTR

CHECK FILES IN THE FOLLOWING DIRECTORY:
%s
================================================================================
''', data_path)

        # Need to specify as 9 or below to log all files
        if int(verbose) < 10:
            logging.debug("Printing filenames...")
            for file in ens_member_files:
                logging.debug("* %s", file)

        # read the data - use dask delayed
        # If testing_mode_n_time == TRUE, only select 10 timesteps
        dset_ens = parallel_or_serial_open_function(
            ens_member_files,
            combine='by_coords',
            compat=merge_compat,
            parallel=parallel,
            chunks={'time': 100, 'lat': 192, 'lon': 288},
        ).isel(isel_time)

        # Preprocess the data
        dset_ens = parallel_or_serial_preprocess_function(
            dset_ens=dset_ens,
            case_name=ens_member,
            preprocess_kwargs=preprocess_kwargs,
            skip_preprocess=skip_preprocess,
            parallel=parallel)

        # Create the task graph of the custom analysis function for lazy eval
        dset_ens = parallel_or_serial_analysis_function(
            dset_ens_preprocessed=dset_ens,
            case_name=ens_member,
            parallel=parallel,
        )

        # not parallelized, simple string manipulation
        ens_member_filename = generate_save_filename(
            save_subdir=new_save_subdirectory,
            save_name=save_name,
            ens_name=ensemble_name,
            ens_member=ens_member,
            nc_file_timestr=nc_file_timestr,
            testing_mode_n_time=testing_mode_n_time
        )

        # SAVE the data to disk
        dset_ens = parallel_or_serial_save_function(
            dset_save=dset_ens,
            ens_member_filename=ens_member_filename,
            parallel=parallel
        )

        # ----------------------------------------------------------------------
        # 2.B.2 Collect results (delayed objects)
        # ----------------------------------------------------------------------

        # Store the delayed objects in a dict for later computation. Use
        # filenames for keys
        analysis_output_list[ens_member_filename] = dset_ens

        icase += 1

    logging.info('COMPLETED Iterating over ensemble members.')

    # --------------------------------------------------------------------------
    # 2.D.parallel Perform delayed computation and collect results
    # --------------------------------------------------------------------------

    if parallel == "TRUE":

        logging.info(
            '''
Performing delayed parallel computation.
Note: the wait here may also indicate that the PBS job is waiting in the job queue.
            '''
        )

        _ = dask.compute(list(analysis_output_list.values()))[0]

    # --------------------------------------------------------------------------
    # 2.D.SERIAL
    # PASS: No action required
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    # 2.E CONCAT RESULTS
    # --------------------------------------------------------------------------

    logging.info('Individual files saved to %s', new_save_subdirectory)
    filenames = list(analysis_output_list.keys())

    if verbose <= 10:
        for file in filenames:
            logging.debug("* %s", file)

    if concat_results == "TRUE":

        logging.info("Concatenating Files...")

        combined_save_name = generate_save_filename(
            save_subdir=new_save_directory,
            save_name=save_name,
            ens_name=ensemble_name,
            nc_file_timestr=nc_file_timestr,
            combined=True,
            n_ens=len(filenames),
            testing_mode_n_time=testing_mode_n_time
        )

        concat_and_save(
            file_list=filenames,
            save_file=combined_save_name,
            casenames=casenames,
            parallel=parallel
        )

    else:

        logging.info("User Flag \"concat_results\" set to \"FALSE\"")

    end_time = datetime.datetime.now()

    time_delta = end_time - start_time

    logging.info('Analysis script complete.')

    logging.info(
        'Analysis script duration (including PBSCluster Queueing):    %s',
        time_delta)

    if parallel == "TRUE":

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
