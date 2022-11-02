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
warnings.filterwarnings("ignore", message="Interpolation point out of data bounds encountered")

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
    
    logging.info("Getting list of variables for import")
    
    netcdf_variables = [
        "Q",  # in kg/kg
        "T",  # in K
        "V",  # in m/s
        "Z3", # in m
        "PS", # need for interp from hybrid to pressure levels
        
        # Add more variables here
    ]
    
    for var in netcdf_variables:
        logging.debug(f" * {var}")
    
    return netcdf_variables

def remap_hybrid_to_pressure(data: xr.DataArray,
                              ps: xr.DataArray,
                              hyam: xr.DataArray,
                              hybm: xr.DataArray,
                              p0: float = 100000.,
                              new_levels: np.ndarray = new_levels,
                              lev_dim: str = None,
                              method: str = 'linear') -> xr.DataArray:
    
    
    new_levels_da = xr.DataArray(
        data   =  new_levels/100,
        coords = {"plev":new_levels/100},
        dims   =  ['plev'],
        attrs  = {'long_name':'pressure','units':'hPa'}
    )
    
    interp_axis = data.dims.index(lev_dim)
    
    # If an unchunked Xarray input is given, chunk it just with its dims
    
    if data.chunks is None:
        
        logging.debug("Rechunking...")
        
        data_chunk = dict([
            (k, v) for (k, v) in zip(list(data.dims), list(data.shape))
        ])
        data = data.chunk(data_chunk)
        
        logging.debug("Chunked...")
        
    # Calculate pressure levels at the hybrid levels
    logging.debug("Calculating pressure from hybrid")
    pressure = _pressure_from_hybrid(ps, hyam, hybm, p0)  # Pa
    
    # Make pressure shape same as data shape
    pressure = pressure.transpose(*data.dims)
    
    # Chunk pressure equal to data's chunks
    logging.debug("Chunking pressure")
    pressure = pressure.chunk(data.chunks)
    
    #abs Output data structure elements
    out_chunks = list(data.chunks)
    out_chunks[interp_axis] = (new_levels.size,)
    out_chunks = tuple(out_chunks)
    
    logging.debug(f"Out Chunks:\n{out_chunks}")

    logging.debug("Mapping the remap function over xr blocks")
    
    output = map_blocks(
                _vertical_remap,
                func_interpolate,
                new_levels,
                pressure.data,
                data.data,
                interp_axis,
                chunks=out_chunks,
                dtype=data.dtype,
                drop_axis=[interp_axis],
                new_axis=[interp_axis],
    )

    logging.debug("Formatting as an xr data array")
    output = xr.DataArray(output,name=data.name,attrs=data.attrs)

    # Set output dims and coords
    dims = [
        data.dims[i] if i != interp_axis else "plev" for i in range(data.ndim)
    ]

    # Rename output dims. This is only needed with above workaround block
    dims_dict = {output.dims[i]: dims[i] for i in range(len(output.dims))}
    output = output.rename(dims_dict)

    coords = {}
    for (k, v) in data.coords.items():
        if k != lev_dim:
            coords.update({k: v})
        else:
            # new_levels = xr.DataArray(new_levels / 100)
            coords.update({"plev": new_levels_da})
            
    logging.debug("Transposing interpolated output")
    output = output.transpose(*dims).assign_coords(coords)
    
    return output

def _vertical_remap(func_interpolate, new_levels, xcoords, data, interp_axis=0):
    """Execute the defined interpolation function on data."""
    
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        
        output = func_interpolate(new_levels, xcoords, data, axis=interp_axis,fill_value=np.nan)

    return output

def _pressure_from_hybrid(psfc, hya, hyb, p0=100000.):
    """Calculate pressure at the hybrid levels."""

    # p(k) = hya(k) * p0 + hyb(k) * psfc

    # This will be in Pa
    return hya * p0 + hyb * psfc


def vertical_coordinate_selection_old(da,ps,hyam,hybm,data_level):
    
    # this function is very inefficient. So far the method is to take an entire 4D array (time,lev,lat,lon) and
    # calculate the 4D pressure field at each hybrid level. Then, the interpolation is performed onto the regular
    # pressure levels specified by geocat.interpolation. Only then is the desired pressure level selected. Not sure if
    # there is a way to get around this, but the "del da_new" statement releases the memor
    
    logging.debug(f'Interpolating from hybrid to pressure levels') 
    
    # Interpolate the data from hybrid (model) coordinates to pressure coordinates
    da_interpolated_to_pressure = gc.interpolation.interp_hybrid_to_pressure(da,ps,hyam,hybm,lev_dim="lev")
    
    logging.info(f"Pressure Levels:\n{da_interpolated_to_pressure.plev}")
    
    logging.info(f"Interpolating to desired presssure level: {data_level*100} Pa")
    
    # Interpolate to desired pressure level (convert data_level in hPa to Pa)
    da_new_specified_level = da_interpolated_to_pressure.interp(plev=data_level*100)
    
    del da_interpolated_to_pressure # Delete this array bc it is huge
    
    logging.info(f"4D Interpolated Array released from memory")
    
    return da_new_specified_level

def vertical_coordinate_selection(da,ps,hyam,hybm,data_level):
    
    # Pressure levels for interpolation (write in hPa then convert to Pa)
    # Note: need at least two levels for interpolation function to not fail,
    # so add a second level for interpolation that is 90% of the desired level
    new_levels = np.array([data_level,data_level*0.9]).astype(np.float32) * 100
    
    logging.info("Performing Vertical Remapping from Hybrid to Pressure Coordinates")
    
    logging.debug(f"Pressure levels (in Pa, not hPa): {new_levels} ")
                                
    # Interpolate from hybrid model levels to pressure levels (now in hPa)
    da_intp = remap_hybrid_to_pressure(
            data = da,
            ps   = ps,
            hyam = hyam,
            hybm = hybm,
            new_levels = new_levels,
            lev_dim = "lev"
    )
    
    return da_intp.interp(plev=data_level)
    

# ==============================================================================
# FUNCTION: custom preprocess function
# 
# Perform the data preprocessing for a single ensemble member
# ==============================================================================

def custom_preprocess_function(dset_ens, case_name, data_level=-1.0,parallel="TRUE"):
    
    preprocess_debugging_text = f'''    
=======================================================================================================================
Debugging text for ensemble member: {case_name}'
=======================================================================================================================

{dset_ens}

--------------------------------------------------------CHUNKS---------------------------------------------------------
{dset_ens.chunks}
-----------------------------------------------------------------------------------------------------------------------
    '''
    
    logging.debug(preprocess_debugging_text)
    

    # Read data from files for 1 ens member
    logging.info("Reading Data")
    ps   = dset_ens.PS.persist()
    v    = dset_ens.V
    t    = dset_ens.T  
    q    = dset_ens.Q    
    z    = dset_ens.Z3    
    hyam = dset_ens.hyam
    hybm = dset_ens.hybm
    
    logging.info("Performing Vertical Coordinate Selection")
    vLEV = vertical_coordinate_selection(v,ps,hyam,hybm,data_level)
    tLEV = vertical_coordinate_selection(t,ps,hyam,hybm,data_level)    
    qLEV = vertical_coordinate_selection(q,ps,hyam,hybm,data_level)    
    zLEV = vertical_coordinate_selection(z,ps,hyam,hybm,data_level)    
    
    vLEV = vLEV.assign_attrs({'units':'m/s',  'long_name':f'Meridional wind at {data_level} hPa'})      
    tLEV = tLEV.assign_attrs({'units':'K',    'long_name':f'Temperature at {data_level} hPa'})     
    qLEV = qLEV.assign_attrs({'units':'kg/kg','long_name':f'Specific humidity at {data_level} hPa'})      
    zLEV = zLEV.assign_attrs({'units':'m',    'long_name':f'Geopotential height at {data_level} hPa'})   
    
    # Create a new dataset to hold data after preprocessing has occurred
    dset_ens_preprocessed = xr.Dataset()
    
    data_level_str = str(int(data_level))
    
    dset_ens_preprocessed[f"V{data_level_str}"] = vLEV
    dset_ens_preprocessed[f"T{data_level_str}"] = tLEV    
    dset_ens_preprocessed[f"Q{data_level_str}"] = qLEV
    dset_ens_preprocessed[f"Z{data_level_str}"] = zLEV    
    
    return dset_ens_preprocessed # output should be an xr dataset

# ==============================================================================
# FUNCTION: custom analysis function
# 
# Perform the primary data analysis for a single ensemble member
# ==============================================================================

def custom_anaylsis_function(dset_ens_preprocessed, case_name, data_level=-1.0,parallel="TRUE"):
    
    logging.debug(f'Performing data analysis for ensemble member: {case_name}')
    logging.debug(dset_ens_preprocessed)
    
    data_level_str = str(int(data_level))
    
    # Read data from preprocessed dataset for 1 ens member
    vLEV = dset_ens_preprocessed[f"V{data_level_str}"]
    tLEV = dset_ens_preprocessed[f"T{data_level_str}"]  
    qLEV = dset_ens_preprocessed[f"Q{data_level_str}"]  
    zLEV = dset_ens_preprocessed[f"Z{data_level_str}"]  
    
    # Calculate terms needed for eddy decomposition
    logging.info("Calculating terms for eddy decomp")
    v_zmean = vLEV.mean(dim='lon')
    t_zmean = tLEV.mean(dim='lon')
    q_zmean = qLEV.mean(dim='lon')
    z_zmean = zLEV.mean(dim='lon')    
    
    v_tmean = vLEV.mean(dim='time')    
    t_tmean = tLEV.mean(dim='time')    
    q_tmean = qLEV.mean(dim='time')    
    z_tmean = zLEV.mean(dim='time')    
    
    t_prm = tLEV - t_tmean
    t_str = tLEV - t_zmean  
    
    q_prm = qLEV - q_tmean
    q_str = qLEV - q_zmean  
    
    z_prm = zLEV - z_tmean
    z_str = zLEV - z_zmean      
    
    v_prm = vLEV - v_tmean
    v_str = vLEV - v_zmean 

    
    # Calculate eddy decomposition
    logging.info("Performing eddy decomp")
    vt_totl = (vLEV  *                         tLEV).mean(dim='time')         * c_p
    vt_stat = (v_str *                         t_str).mean(dim='time')        * c_p
    vt_trns = (v_prm *                         t_prm).mean(dim='time')        * c_p
    vt_mean = (vLEV.mean(dim=('time','lon')) * tLEV.mean(dim=('time','lon'))) * c_p
               
    vq_totl = (vLEV  *                         qLEV).mean(dim='time')         * L_v
    vq_stat = (v_str *                         q_str).mean(dim='time')        * L_v
    vq_trns = (v_prm *                         q_prm).mean(dim='time')        * L_v
    vq_mean = (vLEV.mean(dim=('time','lon')) * qLEV.mean(dim=('time','lon'))) * L_v    
               
    vz_totl = (vLEV  *                         zLEV).mean(dim='time')         * g
    vz_stat = (v_str *                         z_str).mean(dim='time')        * g
    vz_trns = (v_prm *                         z_prm).mean(dim='time')        * g
    vz_mean = (vLEV.mean(dim=('time','lon')) * zLEV.mean(dim=('time','lon'))) * g    
       
    # Add metadata for saving 
    vt_mean = vt_mean.assign_attrs({'units':'J/kg m/s','long_name':f'Mean meridional VT Transport at {data_level} hPa'})      
    vt_stat = vt_stat.assign_attrs({'units':'J/kg m/s','long_name':f'Stationary Eddy Transport of VT at {data_level} hPa'})         
    vt_trns = vt_trns.assign_attrs({'units':'J/kg m/s','long_name':f'Transient Eddy Transport of VT at {data_level} hPa'})               
    vt_totl = vt_totl.assign_attrs({'units':'J/kg m/s','long_name':f'Total VT Transport at {data_level} hPa'})     
    
    vq_mean = vq_mean.assign_attrs({'units':'J/kg m/s','long_name':f'Mean meridional VQ Transport at {data_level} hPa'})      
    vq_stat = vq_stat.assign_attrs({'units':'J/kg m/s','long_name':f'Stationary Eddy Transport of VQ at {data_level} hPa'})         
    vq_trns = vq_trns.assign_attrs({'units':'J/kg m/s','long_name':f'Transient Eddy Transport of VQ at {data_level} hPa'})               
    vq_totl = vq_totl.assign_attrs({'units':'J/kg m/s','long_name':f'Total VQ Transport at {data_level} hPa'})   
      
    vz_mean = vz_mean.assign_attrs({'units':'J/kg m/s','long_name':f'Mean meridional VZ Transport at {data_level} hPa'})      
    vz_stat = vz_stat.assign_attrs({'units':'J/kg m/s','long_name':f'Stationary Eddy Transport of VZ at {data_level} hPa'})         
    vz_trns = vz_trns.assign_attrs({'units':'J/kg m/s','long_name':f'Transient Eddy Transport of VZ at {data_level} hPa'})               
    vz_totl = vz_totl.assign_attrs({'units':'J/kg m/s','long_name':f'Total VZ Transport at {data_level} hPa'})     
                   
    # Create a new dataset to hold data after analysis has occurred
    dset_ens_analyzed = xr.Dataset()
                
    dset_ens_analyzed[f"VT_TOTL"] = vt_totl
    dset_ens_analyzed[f"VT_MEAN"] = vt_mean
    dset_ens_analyzed[f"VT_STAT"] = vt_stat
    dset_ens_analyzed[f"VT_TRNS"] = vt_trns  
               
    dset_ens_analyzed[f"VQ_TOTL"] = vq_totl
    dset_ens_analyzed[f"VQ_MEAN"] = vq_mean
    dset_ens_analyzed[f"VQ_STAT"] = vq_stat
    dset_ens_analyzed[f"VQ_TRNS"] = vq_trns
    
    dset_ens_analyzed[f"VZ_TOTL"] = vz_totl
    dset_ens_analyzed[f"VZ_MEAN"] = vz_mean
    dset_ens_analyzed[f"VZ_STAT"] = vz_stat
    dset_ens_analyzed[f"VZ_TRNS"] = vz_trns                
    
    dset_ens_analyzed[f"T_ZMEAN_TMEAN"] = tLEV.mean(dim=('lon','time'))  
    dset_ens_analyzed[f"Q_ZMEAN_TMEAN"] = qLEV.mean(dim=('lon','time'))  
    dset_ens_analyzed[f"Z_ZMEAN_TMEAN"] = zLEV.mean(dim=('lon','time'))  
    dset_ens_analyzed[f"V_ZMEAN_TMEAN"] = vLEV.mean(dim=('lon','time'))    
    
    if parallel == "FALSE":
        logging.debug("Computing analyzed data...")
        dset_ens_analyzed = dset_ens_analyzed.compute()
    
    return dset_ens_analyzed # output should be an xr dataset

# ==============================================================================
# FUNCTION: custom combination function
# 
# Combine output from every ensemble member into a desired format
# ==============================================================================


def concat_and_save(file_list,casenames,save_file,parallel):
    
    if parallel == "TRUE":
        parallel_or_serial_open_function = dask.delayed(xr.open_dataset)
        
    else:
        parallel_or_serial_open_function = xr.open_dataset
    
    logging.info(f'Combining data from all ensemble members...')
    
    # Empty dict to hold xr objects prior to concat
    dset_dict = {}
    
    # Iterate over files
    for file,case in zip(file_list,casenames):
        logging.debug(f'Reading data from case: {case}')
        dset_dict[case] = parallel_or_serial_open_function(file)
        
    dsets_computed = dask.compute(*dset_dict.values())
        
    # Combine all of the analysis output into a single xr dataset along a new
    # dimension of "ensemble member"
    dset_concat = xr.concat([x for x in dsets_computed],dim='ensemble_member')
    
    # Give appropriate names to the ensemble members, rather than just indexes
    dset_concat = dset_concat.assign_coords({'ensemble_member':casenames})        
    
    logging.info(f'Saving to a single file...')
    
    if parallel == "TRUE":
    
        dset_concat = dset_concat.to_netcdf(save_file,compute=False)
        
        dset_concat.compute()
        
    else:
        
        dset_concat.to_netcdf(save_file,compute=True)
        
    logging.info(f'Data saved to {save_file}')
    
    return      

        
def generate_save_filename(save_subdir,save_name,ens_name,nc_file_timestr,ens_member="",combined=False,n_ens=""):
    
    if combined:
        
        ens_filename = save_subdir + f"{ens_name}_{save_name}_{n_ens}members_{nc_file_timestr}.nc"
        
    else:
        
        ens_filename = save_subdir + f"{ens_name}_{ens_member}_{save_name}_{nc_file_timestr}.nc"        
    
    return ens_filename
        
        
def save_single_ensemble_member(dset_save,ens_member_filename,parallel):  
    
    logging.debug(f"Saving the following dataset:\n{dset_save}")
    
    if parallel == "TRUE":
    
        dset_save = dask.delayed(dset_save.to_netcdf)(ens_member_filename,compute=True)
        
        dset_save.compute()
        
    else:
        
        dset_save = dset_save.to_netcdf(ens_member_filename)
    
    return dset_save
    
