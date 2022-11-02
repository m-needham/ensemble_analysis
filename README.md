# Ensemble Analysis Template

Flexible python script to perform arbitrary analysis of large ensemble model output, with optional parallelization with Dask.

> For instructions on how to use this script, see `INSTRUCTIONS.md`

**Updated 2 November 2022**

This directory contains a template for analyzing large ensembles of climate simulations with a custom analysis function. Most of the code in this directory does not need to change for different analysis goals. Instead it is designed for the user to specify the functions `custom_variable_list`, `custom_preprocess_function`, and `custom_analysis_function` to be applied identically to each ensemble member, and a `custom_save_function` to control the management of output files.

#### Contents

* Python Scripts
    * `_analysis_functions.py`
    * `_ensemble_analysis.py`
    * `_generate_casenames.py`
    * `_user_functions.py`
* Markdown Notes
    * `INSTRUCTIONS.md`
    * `NOTES.md`
    * `README.md`
* Bash Submission Script
    * `submit.sh`

## Overview

The procedure is controlled by the bash script `submit.sh` which takes care of
* Submission to a PBS queue
* Loading the proper python modules
* User specification of the ensemble to analyze
* The chocie of Parallel or Serial computation
* Location to save output files

The `submit.sh` then calls two python scripts:
* `_generate_casenames.py`
    * This script essentially performs string comprehension to generate a unique casename for each member of the ensemble based on the files in the `DATA_PATH` specified in `script.sh`
* `_ensemble_analysis.py`
    * This script contains instructions for the bulk of the analysis. The procedure is:
        * Loop over ensemble members
        * Perform a calculation on each ensemble member
        * Save the results to an output `.nc` file or files
          
`_ensemble_analysis.py` imports many functions from `_analysis_functions.py`, and from `_user_functions.py`. Most user edits are expected to take place in `_user_functions.py`. 



