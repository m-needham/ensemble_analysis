'''Module to generate casenames for a particular large ensemble
'''
# ==============================================================================
# Import Statements
# ==============================================================================

import argparse
import logging
import os
import numpy as np

from _analysis_functions import (
    get_supported_ensembles,
    get_ensemble_data_path
)


def data_freq_compatibility(ensemble_name, data_freq):
    '''Account for slightly different folder names between LENS1, LENS2
    '''

    logging.info(
        "Ensuring compatability between %s and data_freq=%s",
        ensemble_name,
        data_freq)

    if ensemble_name in ["CESM1-LE", "CESM1-SF"]:
        if data_freq == "month_1":

            logging.info(
                "Changing \"month_1\" to \"monthly\" for %s",
                ensemble_name)

            return "monthly"

    return data_freq


def get_ensemble_filters(ensemble_name):
    '''Dict of strings found only in the desired ensemble

    If not necessary, leave empty
    '''

    ensemble_filters = {
        "CESM2-LE": "",
        "CESM2-SF": "",
        "CESM1-LE": "C5CNBDRD",
        "CESM1-SF": "TRLENS_RCP85"
    }

    return ensemble_filters[ensemble_name]


def get_filename_splitters(ensemble_name):
    '''Strings to split filenames for particular ensembles
    '''

    filename_splitters = {
        "CESM2-SF": ["f09_g17.", ".cam."],
        "CESM2-LE": ["f09_g17.", ".cam."],
        "CESM1-LE": ["b.e11.", ".cam."],
        "CESM1-SF": ["b.e11.", ".cam."],
    }

    return filename_splitters[ensemble_name]


def get_filtered_filenames(ensemble_name):
    '''strings for use in filename filtering.

    If True, then include. If False, then exclude
    '''

    filtered_filenames = {
        "CESM1-LE": (["OIC", "xaer", "xbmb", "xghg", "xlulc"], False)
    }

    return filtered_filenames[ensemble_name]

# ==============================================================================
# Generate Case Names
# ==============================================================================


def generate_case_names(path, ensemble_name):
    '''Perform string comprehension to generate parseable case names

    These case names will be used later to generate lists of all files for a
    single case.
    '''

    logging.info(
        "Function: \"generate_case_names\" called with ensemble_name=\"%s\"",
        ensemble_name)

    # List of all files in path (and only use "b" compsets")
    files = np.sort([x for x in os.listdir(path) if ".nc" in x])
    files = np.sort([x for x in files if x[0] == "b"])

    ens_filter = get_ensemble_filters(ensemble_name)

    files = np.sort([x for x in files if ens_filter in x])

    logging.info("* Found %s files", len(files))

    split1, split2 = get_filename_splitters(ensemble_name)

    # Split the string on ".cam."
    cases = []

    i = 0

    for file in files:

        logging.debug("* %s: %s", i + 1, file)

        # get the central string between "fXX_gXX" and ".cam."
        file_tmp = file.split(split1)[1]
        file_tmp = file_tmp.split(split2)[0]

        cases.append(file_tmp)

        i += 1

    cases = list(np.unique(cases))

    return cases


def combine_split_cases(cases, ensemble_name, delimiter="&&"):
    '''Combine historical and forcing-scenario portions of runs into a single case

    For example, in the CESM1 Large ensemble, the historical portions of
    simulations have the string "B20TRC5CNBDRD", while the RCP8.5 portions of
    simulations have the string "BRCP85C5CNBDRD"

    Used only for some large ensembles and not others
    '''

    combined_cases = []

    if ensemble_name == "CESM2-SF":

        # Check for each of AAER, BMB, EE, GHG. Don't need to do xAER

        for forcing in ["AAER", "BMB", "EE", "GHG"]:

            cases_tmp = [case for case in cases if forcing in case]

            case_numbers = [case[-3:] for case in cases_tmp]

            for case_number in case_numbers:

                # Select the cases with the same case id number

                combined_cases_tmp = [
                    case for case in cases_tmp if case_number in case]

                combined_cases.append(delimiter.join(combined_cases_tmp))

        # Lastly, add in the xAER cases
        forcing = "xAER"
        cases_tmp = [case for case in cases if forcing in case]

        for case in cases_tmp:

            combined_cases.append(case)

    elif ensemble_name == "CESM1-LE":

        cases = [x for x in cases if "OIC" not in x]

        case_numbers = [case[-3:] for case in cases]

        for case_number in case_numbers:

            # Ignore cases of the form *.1XX" in favor of cases of the
            # form *.0XX" because of a coordinate mismatch that I don't want to
            # worry about fixing. This action includes 35 ensemble members for
            # analysis and excludes 7 ensemble members

            if case_number[0] == "1":
                logging.debug("Ignoring Case: %s", case_number)

            else:

                combined_cases_tmp = [
                    case for case in cases if case_number in case]

                combined_cases.append(delimiter.join(combined_cases_tmp))

        # return np.unique(cases)

    else:
        return np.unique(cases)

    return np.unique(combined_cases)


# ==============================================================================
# Main Function Call
# ==============================================================================
def main():
    '''Main function call
    '''

    # --------------------------------------------------------------------------
    # Parse command line argument
    # --------------------------------------------------------------------------
    parser = argparse.ArgumentParser()

    parser.add_argument('--data_freq', type=str)
    parser.add_argument('--casenames_file', type=str)
    parser.add_argument('--ensemble_name', type=str)
    parser.add_argument('--use_provided_casenames', type=str)
    parser.add_argument('--verbose', type=str)

    args = parser.parse_args()

    save_file = args.casenames_file
    data_freq = args.data_freq
    ensemble_name = args.ensemble_name.upper()
    use_provided_casenames = args.use_provided_casenames.upper()
    verbose = int(args.verbose)

    # --------------------------------------------------------------------------
    # Initialize Logging
    # --------------------------------------------------------------------------
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        encoding='utf-8',
        level=verbose,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.info("Generating List of Case Names")

    # --------------------------------------------------------------------------
    # Error checking for command line arguments
    # --------------------------------------------------------------------------

    if use_provided_casenames == "TRUE":

        if os.path.exists(save_file):

            use_provided_casenames_logging_text = f'''

======================================================================================================================
USING CASENAMES PROVIDED BY USER
======================================================================================================================
Using casenames provided in {save_file}
                '''

            logging.info(use_provided_casenames_logging_text)

        else:
            use_provided_casenames_logging_text = f'''

======================================================================================================================
!!!WARNING!!!
======================================================================================================================
FLAG \"use_provided_casenames\" SET TO TRUE BUT FILE:
        {save_file}
DOES NOT EXIST
======================================================================================================================
PROCEEDING TO GENERATE CASENAMES AS IF \"use_provided_casenames\" WAS SET TO FALSE
                '''

            logging.warning(use_provided_casenames_logging_text)
            use_provided_casenames = "FALSE"

    elif use_provided_casenames == "FALSE":

        use_provided_casenames_logging_text = f'''
======================================================================================================================
Flag \"use_provided_casenames\" set to FALSE
Generating Casenames for ensemble: {ensemble_name}
======================================================================================================================
        '''

        logging.info(use_provided_casenames_logging_text)

    else:

        use_provided_casenames_logging_text = f'''
======================================================================================================================
!!!WARNING!!!
======================================================================================================================
UNABLE TO INTERPRET FLAG use_provided_casenames = \"{args.use_provided_casenames}\"
PROCEEDING TO GENERATE CASENAMES AS IF \"use_provided_casenames\" WAS SET TO FALSE

'''
        logging.warning(use_provided_casenames_logging_text)
        use_provided_casenames = "FALSE"

    # Check if specified ensemble is supported

    supported_ensembles = get_supported_ensembles()

    if ensemble_name in supported_ensembles:

        logging.info(
            "Ensemble: %s is supported  by this application.", ensemble_name)

    else:

        ensemble_logging_message = F'''
======================================================================================================================
!!!SPECIFIED ENSEMBLE NOT SUPPORTED!!!
======================================================================================================================
THE ENSEMBLE SPEICIFIED BY THE USER:
        ensemble_name={ensemble_name}

IS NOT CURRENTLY SUPPORTED BY THIS APPLICATION. CURRENTLY SUPPORTED ENSEMBLES:
{supported_ensembles}

IF YOU BELIEVE THIS IS IN ERROR, CONFIRM THE ENSEMBLE HAS BEEN ADDED TO BOTH:
    * get_supported_ensembles()
    * get_ensemble_data_path()

EXITING.
        '''

        logging.error(ensemble_logging_message)

        return

    # Only exit if use_provided_casenames=TRUE and has passed the
    # basic error checking, above

    if use_provided_casenames == "TRUE":
        return

    # --------------------------------------------------------------------------
    # Proceed with analysis
    # --------------------------------------------------------------------------

    # Use Temp as an exmaple variable to generate casenames
    data_freq = data_freq_compatibility(ensemble_name, data_freq)
    data_path = get_ensemble_data_path(ensemble_name) + data_freq + "/T"

    # Generate case names
    cases = generate_case_names(data_path, ensemble_name)

    # Function which is necessary for certain ensembles but not for others
    cases = combine_split_cases(cases, ensemble_name)

    # --------------------------------------------------------------------------
    # Save case names to a text files
    # --------------------------------------------------------------------------

    with open(save_file, mode='w', encoding='utf-8') as write_file:
        for case in cases:
            write_file.writelines(case + "\n")

    logging.info("Case names saved to %s", save_file)
    logging.info(
        "Script _generate_casenames.py found %s Ensemble Members.", len(cases))

    # Return the file name to use in later part of the script
    return


if __name__ == "__main__":
    main()
