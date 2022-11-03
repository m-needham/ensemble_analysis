'''Test set of user functions

Author: Michael R. Needham (m.needham@colostate.edu)
Last Updated: 3 November 2022

These functions are placeholders to ensure that the full ensemble analysis
program works correctly for an arbitrary analysis question.

The test function is to calculate the brightness temperature from outgoing
longwave radiation for each ensemble member, following the Stefan-Boltzmann law:

                     F = sigma * temperature ^ 4
'''

# ==============================================================================
# Import Statements
# ==============================================================================


import logging
import warnings

import dask
import numpy as np
import xarray as xr

from scipy.interpolate import interp1d
from xarray import map_blocks


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
        "FLNT"
    ]

    for var in netcdf_variables:
        logging.debug(" * %s", var)

    return netcdf_variables


# ==============================================================================
# FUNCTION: custom preprocess function
#
# Perform the data preprocessing for a single ensemble member
# ==============================================================================
def parse_preprocess_kwargs(preprocess_kwargs):
    '''Function to parse preprocess_kwargs into a dictionary

    Assumed format is:
        * name1&&value1_name2&&value2

    Output will be a dictionary like:
        dict[name1] = value1
        dict[name2] = value2
        ...
        dict[nameN] = valueN

    For an arbitrary number of key/val pairs
    '''

    # Get a list of
    kwarg_pairs = preprocess_kwargs.split("_")

    kwarg_dict = {}
    for pair in kwarg_pairs:
        key, val = pair.split("&&")

        kwarg_dict[key] = val

    return kwarg_dict


def custom_preprocess_function(
        dset_ens,
        case_name,
        preprocess_kwargs,
        parallel="TRUE",
        skip_preprocess="TRUE"
):
    '''Function to preprocess data prior to analysis

THE INPUT "dset_ens" INCLUDES DATA FOR A SINGLE ENSEMBLE MEMBER.

THE OUTPUT "dset_ens_preprocessed" INCLUDES DATA FOR A SINGLE ENSEMBLE
MEMBER, WHICH HAS BEEN PREPROCESSED. EVEN IF ONLY ONE VARIABLE IS USED, IT
IS RECOMMENDED TO PASS THE VARIABLE WITHIN A DATASET INSTEAD OF WITHIN A
DATAARRAY, OTHERWISE CHANGES MAY NEED TO BE MADE ELSEWHERE.

THE DEFAULT BEHAVIOR IS SIMPLY TO PASS INPUT DATA ALONG

Include notes here on specifically how the preprocessing takes place, if at all
    '''

    if skip_preprocess == "TRUE":

        logging.info("Flag \"skip_preprocess\"=\"TRUE\"")

        return dset_ens

    logging.debug('''
================================================================================
Debugging text for ensemble member: %s'
================================================================================

%s

-------------------------------------CHUNKS-------------------------------------
%s
--------------------------------------------------------------------------------
    ''', case_name, dset_ens, dset_ens.chunks)

    logging.debug('Flag \"PARALLEL\" set to %s', parallel)

    # Parse kwargs for preprocessing step
    kwarg_dict = parse_preprocess_kwargs(preprocess_kwargs)

    logging.debug("Preprocessing Arguements:")
    for key, val in zip(kwarg_dict.keys(), kwarg_dict.values()):
        logging.debug("* %s %s", key, val)

    # Create a new dataset to hold data after preprocessing has occurred
    dset_ens_preprocessed = xr.Dataset()

    # ===== BEGINNING OF CUSTOM PREPROCESSING CODE ===========================

    #
    #
    #

    # ===== END OF CUSTOM PREPROCESSING CODE =================================

    return dset_ens_preprocessed  # output should be an xr dataset

# ==============================================================================
# FUNCTION: custom analysis function
#
# Perform the primary data analysis for a single ensemble member
# ==============================================================================


def custom_anaylsis_function(
        dset_ens_preprocessed,
        case_name,
        parallel="TRUE"):
    '''Custom function to analyze data


    THE INPUT "dset_ens_preprocessed" INCLUDES DATA FOR A SINGLE ENSEMBLE MEMBER.

    THE OUTPUT "dset_ens_preprocessed" INCLUDES DATA FOR A SINGLE ENSEMBLE
    MEMBER, WHICH HAS BEEN PREPROCESSED. EVEN IF ONLY ONE VARIABLE IS USED, IT
    IS RECOMMENDED TO PASS THE VARIABLE WITHIN A DATASET INSTEAD OF WITHIN A
    DATAARRAY, OTHERWISE CHANGES MAY NEED TO BE MADE ELSEWHERE.
    '''

    logging.debug(
        'Performing data analysis for ensemble member: %s',
        case_name)
    logging.debug(dset_ens_preprocessed)

    # Create a new dataset to hold data after analysis has occurred
    dset_ens_analyzed = xr.Dataset()

    # ===== BEGINNING OF CUSTOM ANALYSIS CODE ================================

    # Get metadata for creating xr data array
    coords = dset_ens_preprocessed["FLNT"].coords
    dims = dset_ens_preprocessed["FLNT"].dims

    # Calculate brightness temperature
    temp_brt = xr.DataArray(
        data=(dset_ens_preprocessed["FLNT"] / (5.67e-8)) ** 0.25,
        coords=coords,
        dims=dims,
        attrs={
            'long_name': 'brightness temperature from FLNT',
            'units': 'K'
        }
    )

    # store in analyzed dataset, along with FLNT
    dset_ens_analyzed["FLNT"] = dset_ens_preprocessed["FLNT"]
    dset_ens_analyzed["TBRT"] = temp_brt

    # ===== END OF CUSTOM ANALYSIS CODE ======================================

    if parallel == "FALSE":
        logging.debug("PARALLEL=FALSE: Computing analyzed data...")
        dset_ens_analyzed = dset_ens_analyzed.compute()

    return dset_ens_analyzed  # output should be an xr dataset

# ==============================================================================
# FUNCTION: rremap_hybrid_to_pressure
#
# Perform the primary data analysis for a single ensemble member
# ==============================================================================


# Pressure levels for interpolation (write in hPa then convert to Pa)
standard_levels = np.array([
    1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100
]).astype(np.float32) * 100


def remap_hybrid_to_pressure(data: xr.DataArray,
                             p_s: xr.DataArray,
                             hyam: xr.DataArray,
                             hybm: xr.DataArray,
                             p_0: float = 100000.,
                             new_levels: np.ndarray = standard_levels,
                             lev_dim: str = None) -> xr.DataArray:
    '''Function to remap hybrid model levels to pressure levels

    Originally taken from the package geocat-comp, from the function:

    geocat.comp.interpolation.interp_hybrid_to_pressure

    See https://geocat-comp.readthedocs.io/en/latest/ for details


    '''

    # Suppress a metpy interpolation warning
    warnings.filterwarnings(
        "ignore",
        message="Interpolation point out of data bounds encountered"
    )

    func_interpolate = interp1d

    new_levels_da = xr.DataArray(
        data=new_levels / 100,
        coords={"plev": new_levels / 100},
        dims=['plev'],
        attrs={'long_name': 'pressure', 'units': 'hPa'}
    )

    interp_axis = data.dims.index(lev_dim)

    # If an unchunked Xarray input is given, chunk it just with its dims

    if data.chunks is None:

        logging.debug("Rechunking...")

        data_chunk = dict(list(zip(list(data.dims), list(data.shape))))
        data = data.chunk(data_chunk)

        logging.debug("Chunked...")

    # Calculate pressure levels at the hybrid levels
    logging.debug("Calculating pressure from hybrid")
    pressure = _pressure_from_hybrid(p_s, hyam, hybm, p_0)  # Pa

    # Make pressure shape same as data shape
    pressure = pressure.transpose(*data.dims)

    # Chunk pressure equal to data's chunks
    logging.debug("Chunking pressure")
    pressure = pressure.chunk(data.chunks)

    # abs Output data structure elements
    out_chunks = list(data.chunks)
    out_chunks[interp_axis] = (new_levels.size,)
    out_chunks = tuple(out_chunks)

    logging.debug("Out Chunks:\n%s", out_chunks)

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
    output = xr.DataArray(output, name=data.name, attrs=data.attrs)

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


def _vertical_remap(
        func_interpolate,
        new_levels,
        xcoords,
        data,
        interp_axis=0):
    """Execute the defined interpolation function on data."""

    with dask.config.set(**{'array.slicing.split_large_chunks': True}):

        output = func_interpolate(
            new_levels,
            xcoords,
            data,
            axis=interp_axis,
            fill_value=np.nan)

    return output


def _pressure_from_hybrid(p_sfc, hya, hyb, p_0=100000.):
    """Calculate pressure at the hybrid levels."""

    # p(k) = hya(k) * p_0 + hyb(k) * p_sfc

    # This will be in Pa
    return hya * p_0 + hyb * p_sfc
