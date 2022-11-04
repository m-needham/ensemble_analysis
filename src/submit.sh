#!/bin/bash -l
### NAME OF JOB FOR THE QUEUE
#PBS -N ens_250_sub      

### ACCOUNT NUMBER
#PBS -A UHAR0008                    

### SPECIFY COMPUTING RESOURCES 
### NOTE: Only request a small amount here because
### the majority of the computing is done on a separate
### Dask cluster that is initialized by the script
#PBS -l select=1:ncpus=1:mem=256GB   

### SPECIFIY JOB MAX WALLTIME
#PBS -l walltime=12:00:00           

### USE CASPER JOB QUEUE
#PBS -q casper     

# JOIN OUTPUT AND ERROR STREAMS INTO A SINGLE FILE 
#PBS -j oe                          
#------------------------------------------------------------------------------

echo "Beginning ensemble analysis script"

export TMPDIR=/glade/scratch/$USER/temp
mkdir -p $TMPDIR

# Ensure the proper python environment is active
module del python
module load conda/latest
conda activate py_ucar

# -----GLOBAL VARIABLES FOR ALL SCRIPTS----------------------------------------

# CASENAMES_FILE:  Name of local text file to hold casenames. See USE_PROVIDED_CASENAMES, below
# DATA_FREQ:       Time frequency for input data (see README for details)
# ENSEMBLE_NAME:   String identifier to help with functions. See _analysis_functions.py for a list of supported members
# JOB_SCHEDULER:   Type of system for the dask cluster
# NC_FILE_TIMESTR: Additional string to filter nc files. Useful for only calculating on high-temporal-resolution data. To ignore, use "NONE"
# PARALLEL:        (valid: "TRUE", "FALSE") Use Parallel or Serial computing 
# PREPROCESS_KWARGS: Expected format is "name1&&value1_name2&&value2_"
# SAVE_PATH:       Location to store output files
# SAVE_NAME:       String identifier for output files
# SKIP_ANALYSIS:   (valid: "TRUE", "FALSE") If TRUE, only run _generate_casenames.py
# TESTING_MODE_N_ENS:    (valid: "TRUE", "FALSE") If "TRUE", perform analysis on only two ensemble members
# TESTING_MODE_N_TIME:   (valid: "TRUE", "FALSE") If "TRUE", perform analysis on only 10 timesteps from each ensemble member
# USE_PROVIDED_CASENAMES: Use casenames provided by user in CASENAMES_FILE
# VERBOSE:         Output level for log file (10 - debug, 20 - info, 30 - warning, 40 - error)

CASENAMES_FILE="casenames.txt"
CONCAT_RESULTS="TRUE"
DATA_FREQ="month_1"
ENSEMBLE_NAME="CESM2-LE"
JOB_SCHEDULER="NCAR"
NC_FILE_TIMESTR="NONE" # 18500101-18591231
PARALLEL="TRUE"
PREPROCESS_KWARGS="datalev&&250"
SAVE_PATH="/glade/work/$USER/data_misc/ens_analysis/test/"
SAVE_FIELDNAME="brightness_temperature" 
SAVE_NAME="${DATA_FREQ}_${SAVE_FIELDNAME}" 
SKIP_ANALYSIS="FALSE"
SKIP_PREPROCESS="TRUE"
TESTING_MODE_N_ENS="TRUE"
TESTING_MODE_N_TIME="FALSE"
USE_PROVIDED_CASENAMES="FALSE"
VERBOSE="20" 


# -----PERFORM ANALYSIS WITH PYTHON SCRIPTS------------------------------------

# 1. GENERATE A LIST OF CASENAMES FROM THE SPECIFIED ENSEMBLE
python3 _generate_casenames.py --casenames_file $CASENAMES_FILE --data_freq $DATA_FREQ --ensemble_name $ENSEMBLE_NAME --use_provided_casenames $USE_PROVIDED_CASENAMES

# 2. PERFORM THE PRIMARY DATA ANALYSIS
python3 _ensemble_analysis.py --casenames_file $CASENAMES_FILE --concat_results $CONCAT_RESULTS --data_freq $DATA_FREQ --ensemble_name $ENSEMBLE_NAME --job_scheduler $JOB_SCHEDULER --nc_file_timestr $NC_FILE_TIMESTR --parallel $PARALLEL --preprocess_kwargs $PREPROCESS_KWARGS --save_path $SAVE_PATH --save_name $SAVE_NAME --skip_analysis $SKIP_ANALYSIS --skip_preprocess $SKIP_PREPROCESS --testing_mode_n_ens $TESTING_MODE_N_ENS --testing_mode_n_time $TESTING_MODE_N_TIME --user $USER --verbose $VERBOSE 

echo "Removing logs from dask PBS jobs"
rm dask-worker*

echo "Finished ensemble analysis script"


