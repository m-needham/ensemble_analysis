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
    