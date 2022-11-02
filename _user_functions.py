'''Custom user functions

Author: Michael R. Needham (m.needham@colostate.edu)
Last Updated: 2 November 2022

These functions will likely be updated for each different analysis task
'''

# ==============================================================================
# Import Statements
# ==============================================================================


import logging
import xarray as xr

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
    '''List of variables for import'''

    logging.info("Getting list of variables for import")

    # Variables to be read-in
    netcdf_variables = [
        "Q",  # in kg/kg
        "T",  # in K
        "V",  # in m/s
        "Z3",  # in m
        "PS",  # need for interp from hybrid to pressure levels

        # Add more variables here
    ]

    for var in netcdf_variables:
        logging.debug(" * %s", var)

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
    '''Function to preprocess data prior to analysis'''

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

    return dset_ens_preprocessed  # output should be an xr dataset

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
    '''Custom function to analyze data'''

    logging.debug(
        'Performing data analysis for ensemble member: %s',
        case_name)
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

    return dset_ens_analyzed  # output should be an xr dataset
