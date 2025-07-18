import xarray as xr
import cftime
import pandas as pd
import os
from datetime import timedelta
import numpy as np
from pathlib import Path
import tempfile
import threading
import time
 
def update_var_list(var_list,run_date):
    var_metadata = {
        'salinity': {
            "var_id": "salinity",
            "dataset": "https://tds.hycom.org/thredds/dodsC/FMRC_ESPC-D-V02_s3z/FMRC_ESPC-D-V02_s3z_best.ncd",
            "fname": f"hycom_salinity_{run_date.strftime('%Y%m%d_%H')}.nc",
            },
        'water_temp': {
            "var_id": "water_temp",
            "dataset": "https://tds.hycom.org/thredds/dodsC/FMRC_ESPC-D-V02_t3z/FMRC_ESPC-D-V02_t3z_best.ncd",
            "fname": f"hycom_water_temp_{run_date.strftime('%Y%m%d_%H')}.nc",
            },
        'surf_el': {
            "var_id": "surf_el",
            "dataset": "https://tds.hycom.org/thredds/dodsC/FMRC_ESPC-D-V02_ssh/FMRC_ESPC-D-V02_ssh_best.ncd",
            "fname": f"hycom_surf_el_{run_date.strftime('%Y%m%d_%H')}.nc",
            },
        'water_u': {
            "var_id": "water_u",
            "dataset": "https://tds.hycom.org/thredds/dodsC/FMRC_ESPC-D-V02_u3z/FMRC_ESPC-D-V02_u3z_best.ncd",
            "fname": f"hycom_water_u_{run_date.strftime('%Y%m%d_%H')}.nc",
            },
        'water_v': {
            "var_id": "water_v",
            "dataset": "https://tds.hycom.org/thredds/dodsC/FMRC_ESPC-D-V02_v3z/FMRC_ESPC-D-V02_v3z_best.ncd",
            "fname": f"hycom_water_v_{run_date.strftime('%Y%m%d_%H')}.nc",
            }
        }
    return {var: var_metadata[var] for var in var_list if var in var_metadata}

def decode_time_units(time_var):
    try:
        units = time_var.units
        calendar = time_var.calendar
        time = cftime.num2date(
            time_var[:], units=units, calendar=calendar,
            only_use_cftime_datetimes=False, only_use_python_datetimes=True
            )
        return pd.DatetimeIndex(time)      
    except AttributeError as e:
        raise ValueError(f"Missing expected attributes in time_var: {e}")
    except Exception as e:
        raise RuntimeError(f"Error decoding time units: {e}")

def download_hycom(dataset, var, start_date, end_date, domain, depths, outputDir, fname):
    vars_to_drop = ['salinity_bottom', 'water_temp_bottom', 'water_u_bottom', 'water_v_bottom', 'tau', 'time_offset',
                    'time_run', 'time1_offset', 'sst', 'sss', 'ssu', 'ssv', 'sic', 'sih', 'siu', 'siv', 'surtx',
                    'surty', 'steric_ssh']
    
    # define spatial subsets
    lon_range, lat_range = slice(domain[0], domain[1]), slice(domain[2], domain[3])
    
    if len(depths) > 1 : depth_range = slice(depths[0], depths[1])
    else: depth_range = depths[0]
    
    print('')
    print(f'Downloading: {var}')
    
    # Because of xarray (and netCDF4) lazyt way of loading files using Opendap, sometimes they 
    # do not read in the correct times. This leads to issues when subsetting. 
    # Therefore, we impose a loop to ensure that it does load 
    # in the files correctly before continuing with the subsetting and downloading.
    if 'surf_el' in var: Nt = 385 # hourly for ssh
    else: Nt = 121                # three hourly for the rest of the components
    i = 1
    MAX_TRIES = 100
    success = False
    while i <= MAX_TRIES:
        try:
            ds = xr.open_dataset(
                dataset,
                drop_variables=vars_to_drop,
                decode_times=False
                ).sel(lat=lat_range, lon=lon_range)
            try:
                ds['time'] = decode_time_units(ds['time'])
                print(f"[Try {i}] Decoded the times.")
                if np.unique(ds['time']).size >= Nt:
                    success = True
                    break  # Exit the loop if we have the right number of time steps
                else:
                    print(f"[Try {i}] Incomplete time coverage.")
                    ds.close()
                    i += 1
            except Exception as e:
                print(f"[Try {i}] Time decoding failed: {e}")
                ds.close()
                i += 1
        except Exception as e:
            print(f"[Try {i}] Dataset open failed: {e}")
            i += 1
    if not success:
        raise RuntimeError(f"Failed to open and decode the dataset correctly after {MAX_TRIES} attempts.")

    variable = ds[var].sel(time=slice(start_date,end_date))
            
    if variable.ndim == 4: variable = variable.sel(depth=depth_range)

    variable = variable.resample(time='1D').mean()
    
    tmp_dir = Path(tempfile.mkdtemp())
    time_slices = []
    
    for t in range(variable.time.values.size):
        try:
            # Save temporary file
            time_str = pd.to_datetime(variable.time.values[t]).strftime("%Y-%m-%d")
            tmp_file = tmp_dir / f"{var}_{time_str}.nc"
            v=variable[t]
            v.to_netcdf(tmp_file)
            time_slices.append(tmp_file)
        except Exception as e:
            print(f"Failed to download time {t}: {e}")
   
    # Combine time slices
    datasets = [xr.open_dataset(f) for f in time_slices]
    combined = xr.concat(datasets, dim="time")
    combined = combined.sortby('time')

    save_path = os.path.join(outputDir, fname)
    combined=combined.sel(time=slice(start_date, end_date))
    combined.to_netcdf(save_path)

    ds.close()
    for f in time_slices:
        f.unlink()

def download_hycom_ops(domain, run_date, hdays, fdays, outputDir, parallel = False):
    """
    Downloads the HYCOM analysis variables (salinity, water_temp, surf_el, water_u and water_v) required 
    to run our forecast models. The variables are stored in daily outputs.

    INPUTS:
    domain    : List of geographical coordinates to subset the data and download (e.g. [lon_min,lon_max,lat_min,lat_max]).
    run_date  : Todays datetime to download (e.g. datetime.datetime(YYYY,MM,DD)).
    hdays     : Days to hindcast (e.g. hdays=5).
    fdays     : Days to forecast (e.g. fdays=5).
    outputDir : Directory to save the downloaded data (eg. outputDir='/path/and/directory/to/save/').
    parallel  : Default is False = downloading in series. True = parallel download. 
    OUTPUT:
    NetCDF file containing the most recent HYCOM forcast run.
    """

    start_time = time.time()

    # We add an additional day to ensure that it exceeds the model run time.    
    hdays,fdays = hdays + 1, fdays + 1
    start_date = pd.Timestamp(run_date) - timedelta(days=hdays)
    end_date = pd.Timestamp(run_date) + timedelta(days=fdays)
    
    # This function creates a metadata dictionary which comtains information about the variables. 
    # we are intersted in downloading
    VARIABLES = update_var_list(['salinity', 'water_temp', 'surf_el', 'water_u', 'water_v'],
                                run_date)
    
    # Define depth to download
    depths=[0,5000]
    
    # Downloading in series
    if not parallel:
        for var in VARIABLES:
            download_hycom(VARIABLES[var]["dataset"], 
                           VARIABLES[var]["var_id"], 
                           start_date, 
                           end_date, 
                           domain, 
                           depths, 
                           outputDir, 
                           VARIABLES[var]["fname"]
                           )
    
    # Downloading in parallel
    else:
        def download_worker(var):
            download_hycom(VARIABLES[var]["dataset"], 
                           VARIABLES[var]["var_id"], 
                           start_date, 
                           end_date, 
                           domain, 
                           depths, 
                           outputDir, 
                           VARIABLES[var]["fname"]
                           )
        threads = []
    
        for var in VARIABLES:
            t = threading.Thread(target=download_worker, args=(var,))
            threads.append(t)
            t.start()
            time.sleep(2)
        # Wait for all threads to finish
        for t in threads:
            t.join()
        
    # Combine the variables into one file
    ds = xr.open_mfdataset(os.path.join(outputDir, 'hycom_*.nc'))
        
    outfile = os.path.abspath(os.path.join(outputDir, f"HYCOM_{run_date.strftime('%Y%m%d_%H')}.nc"))
    
    if os.path.exists(outfile): os.remove(outfile)
    
    ds.to_netcdf(outfile, 'w')
    os.chmod(outfile, 0o775)

    print(f'\nCreated {outfile} successfully.')
    print(f'\nFiles downloaded in {timedelta( seconds = time.time() - start_time )} [hh:mm:ss].')
    print('')

if __name__ == '__main__':
    run_date = pd.to_datetime('2025-07-18 00:00:00')
    hdays = 0
    fdays = 0
    domain = [11,36,-39,-25]
    domain = [11,32,-39,-38]
    outputDir = '/home/g.rautenbach/Projects/somisana-croco/DATASETS_CROCOTOOLS/HYCOM/'
    parallel = True
    download_hycom_ops(domain, run_date, hdays, fdays, outputDir, parallel)
      
