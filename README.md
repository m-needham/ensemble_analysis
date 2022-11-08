# Ensemble Analysis Template

Flexible python script to perform arbitrary analysis of large ensemble model output, with optional parallelization with Dask.

> For instructions on how to use this script, see `INSTRUCTIONS.md`

**Updated 8 November 2022**

This directory contains a template for analyzing large ensembles of climate simulations with a custom analysis function. Most of the code in this directory does not need to change for different analysis goals. Instead it is designed for the user to specify the functions `custom_variable_list`, `custom_preprocess_function`, and `custom_analysis_function` to be applied identically to each ensemble member.

#### Contents

* Python Scripts
    * `./src/_analysis_functions.py`
    * `./src/_ensemble_analysis.py`
    * `./src/_generate_casenames.py`
    * `./src/_user_functions_test_set.py`
* Markdown Notes
    * `INSTRUCTIONS.md`
    * `README.md`
* Bash Scripts
    * `./src/submit.sh`
    * `./create_project.sh`
    

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

## Test functions

The python file `_user_functions_test_set.py` contains a minimum working example of the capabilities of this script. When the following bash script is run:

```bash
bash create_project.sh PROJECT_NAME
``` 

`_user_functions_test_set.py` is copied to the newly-created directory `/PROJECT_NAME` and renamed to `_user_functions.py`, where it can be fun by the submit script.

>The testing example inputs outgoing longwave radiation data (variable name `FLNT`), and calculates brightness temperature following the [Stefan-Boltzmann Law](https://en.wikipedia.org/wiki/Stefan%E2%80%93Boltzmann_law):
$$
F = \sigma T_B^4 \quad \rightarrow \quad T_B = \Big(\frac{F}{\sigma}\Big)^\frac{1}{4}
$$
Where $\sigma=5.67\times10^{-8}$ is the Stefan-Boltzmann constant.



