#!/bin/bash -l
### NAME OF JOB FOR THE QUEUE
#PBS -N PLACEHOLDER_JOBNAME      

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

# ==============================================================================
# ==============================================================================

# =========================== PRIMARY USER VARIABLES ===========================
CONCAT_RESULTS="TRUE"
DATA_FREQ="month_1"
ENSEMBLE_NAME="CESM1-SF"
JOB_SCHEDULER="NCAR"
NC_FILE_TIMESTR="NONE" # 18500101-18591231
REMOVE_DASK_LOGS="FALSE"
PARALLEL="FALSE"
PREPROCESS_KWARGS="datalev&&250"
SKIP_ANALYSIS="TRUE"
SKIP_PREPROCESS="TRUE"
USE_PROVIDED_CASENAMES="FALSE"
VERBOSE="20" 
SAVE_FIELDNAME="brightness_temperature" 
USER_FILE_PATH=$PWD

# ================================ TESTING MODE ================================
TESTING_MODE_N_ENS="TRUE"
TESTING_MODE_N_TIME="TRUE"

# ============================ DOWNSTREAM VARIABLES ============================
CASENAMES_FILE="casenames_${ENSEMBLE_NAME}.txt"
SAVE_NAME="${DATA_FREQ}_${SAVE_FIELDNAME}" 
SAVE_PATH="/glade/work/$USER/data_misc/ensemble_analysis/$SAVE_FIELDNAME/"


# ==============================================================================
# ==============================================================================

# ----------------------- Ensure desired save path exists ----------------------
if [ -d $SAVE_PATH ]
    then
    echo "Files will be saved to existing save directory:"
    
    else
    echo "Making New Save Directory:"
    mkdir -p $SAVE_PATH
fi
echo "  $SAVE_PATH"


# -----PERFORM ANALYSIS WITH PYTHON SCRIPTS------------------------------------

# 1. GENERATE A LIST OF CASENAMES FROM THE SPECIFIED ENSEMBLE
python3 ../src/_generate_casenames.py --casenames_file $CASENAMES_FILE --data_freq $DATA_FREQ --ensemble_name $ENSEMBLE_NAME --use_provided_casenames $USE_PROVIDED_CASENAMES --verbose $VERBOSE

# 2. PERFORM THE PRIMARY DATA ANALYSIS
python3 ../src/_ensemble_analysis.py --casenames_file $CASENAMES_FILE --concat_results $CONCAT_RESULTS --data_freq $DATA_FREQ --ensemble_name $ENSEMBLE_NAME --job_scheduler $JOB_SCHEDULER --nc_file_timestr $NC_FILE_TIMESTR --parallel $PARALLEL --preprocess_kwargs $PREPROCESS_KWARGS --save_path $SAVE_PATH --save_name $SAVE_NAME --skip_analysis $SKIP_ANALYSIS --skip_preprocess $SKIP_PREPROCESS --testing_mode_n_ens $TESTING_MODE_N_ENS --testing_mode_n_time $TESTING_MODE_N_TIME --user $USER --user_file_path $USER_FILE_PATH --verbose $VERBOSE 

# Remove dask worker jobs if run in parallel
if [ $PARALLEL == "TRUE" ]
    then
    if [ $REMOVE_DASK_LOGS == "TRUE" ]
        then
        echo "Removing logs from dask PBS jobs"
        rm dask-worker*
        
        else
        echo "Retaining dask logs"  
    fi
fi

echo "Finished ensemble analysis script"


