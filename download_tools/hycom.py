import xarray as xr
import cftime
import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path
import tempfile
import threading
import calendar
from glob import glob
from time import sleep

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
        times = cftime.num2date(
            time_var[:], units=units, calendar=calendar,
            only_use_cftime_datetimes=False, only_use_python_datetimes=True
            )
        return pd.DatetimeIndex(times)      
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
    
    # Because of xarray (and netCDF4) lazy way of loading files using Opendap, sometimes it 
    # does not read in the times correctly, hence the lazy loading. This leads to issues when subsetting. 
    # Therefore, we impose a loop to ensure that it does load in the files correctly before continuing 
    # with the subsetting and downloading.
    if 'surf_el' in var: Nt = 361 # hourly for ssh
    else: Nt = 121                # three hourly for water_temp, salinity, water_u and water_v

    if Path(outputDir, fname).exists(): print(f'\n{fname} already exist.\nDownload skipped.\n')
    else:
        i = 1
        MAX_TRIES = 100
        success = False
        while i <= MAX_TRIES:
            # Phase 1: open dataset and verify time coverage
            ds = None
            try:
                ds = xr.open_dataset(
                    dataset,
                    drop_variables=vars_to_drop,
                    decode_times=False
                    ).sel(lat=lat_range, lon=lon_range)
                try:
                    ds['time'] = decode_time_units(ds['time'])
                    print(f"[Try {i}] Decoded the times.")
                    if np.unique(ds['time']).size < Nt:
                        print(f"[Try {i}] Incomplete time coverage.")
                        ds.close()
                        i += 1
                        sleep(5)
                        continue
                except Exception as e:
                    print(f"[Try {i}] Time decoding failed: {e}")
                    ds.close()
                    i += 1
                    sleep(5)
                    continue
            except Exception as e:
                print(f"[Try {i}] Dataset open failed: {e}")
                if ds is not None:
                    ds.close()
                i += 1
                sleep(5)
                continue

            # Phase 2: download timesteps and validate data
            variable = ds[var].sel(time=slice(start_date,end_date))

            if variable.ndim == 4: variable = variable.sel(depth=depth_range)

            variable = variable.resample(time='1D').mean()

            tmp_dir = Path(tempfile.mkdtemp())
            time_slices = []

            try:
                has_nan = False
                for t in range(variable.time.values.size):
                    try:
                        # Save temporary file
                        time_str = pd.to_datetime(variable.time.values[t]).strftime("%Y-%m-%d")
                        tmp_file = tmp_dir / f"{var}_{time_str}.nc"
                        v=variable[t]
                        v.load()
                        # Check if the data is all NaN (server returned fill values)
                        if np.all(np.isnan(v.values)):
                            print(f"[Try {i}] WARNING: {var} at {time_str} is all NaN")
                            has_nan = True
                            break
                        v.to_netcdf(tmp_file)
                        time_slices.append(tmp_file)
                    except Exception as e:
                        print(f"Failed to download time {t}: {e}")

                if has_nan:
                    print(f"[Try {i}] Data not fully available yet. Retrying in 5 minutes...")
                    i += 1
                    sleep(300)
                    continue

                # Combine time slices
                with xr.open_mfdataset(time_slices, combine='by_coords') as combined:
                    combined = combined.sortby('time')
                    save_path = os.path.join(outputDir, fname)
                    combined = combined.sel(time=slice(start_date, end_date))
                    combined.to_netcdf(save_path)
                success = True
                break

            finally:
                ds.close()
                for f in time_slices:
                    f.unlink()
                if tmp_dir.exists():
                    tmp_dir.rmdir()

        if not success:
            raise RuntimeError(f"Failed to download valid data for {var} after {MAX_TRIES} attempts.")

def download_hycom_ops(domain, run_date, hdays, fdays, outputDir, parallel=True):
    """
    Downloads the HYCOM analysis variables (salinity, water_temp, surf_el, water_u and water_v) required 
    to run our forecast models. The variables are stored in daily outputs.

    INPUTS:
    domain    : List of geographical coordinates to subset the data and download (e.g. [lon_min,lon_max,lat_min,lat_max]).
    run_date  : Todays datetime to download (e.g. datetime.datetime(YYYY,MM,DD)).
    hdays     : Days to hindcast (e.g. hdays=5).
    fdays     : Days to forecast (e.g. fdays=5).
    outputDir : Directory to save the downloaded data (eg. outputDir='/path/and/directory/to/save/').
    parallel  : Default is True = parallel download. False = downloading in series.
    OUTPUT:
    NetCDF file containing the most recent HYCOM forcast run.
    """

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
            sleep(2)
        # Wait for all threads to finish
        for t in threads:
            t.join()
        
    output_dir = Path(outputDir)  
    files = sorted(output_dir.glob("hycom_*.nc"))
    # We ensure that all the variables have been saved before merginf it.
    # If there is a file missing, then the function will fail. 
    # in our operational workflow, it will restart the download automatically. 
    if len(files) == 5:       
        with xr.open_mfdataset(files, combine="by_coords") as ds:
            outfile = output_dir / f"HYCOM_{run_date.strftime('%Y%m%d_%H')}.nc"
            
            # Remove if exists
            if outfile.exists(): outfile.unlink()
            
            ds.to_netcdf(outfile, mode="w")
            outfile.chmod(0o775)
        
        print("\nFiles downloaded successfully.")
        print(f"\nCreated {outfile} successfully.\n")
    
    else:
        raise RuntimeError(f"Expected 5 files, found {len(files)} — download/s may have failed.")

def _download_day(dataset_url, day_start, day_end, var_list, depth_range,
                  surface, lon_range, lat_range, vars_to_drop, tmp_dir):
    """
    Download a single day's data by opening its own OPeNDAP connection.
    Each thread gets an independent netCDF4 handle to avoid segfaults.
    Returns the path to the temp file, or None on failure.
    """
    day_str = day_start.strftime('%Y-%m-%d')
    tmp_file = os.path.join(tmp_dir, f'{day_str}.nc')

    # skip if already downloaded (e.g. from a previous partial run)
    if os.path.exists(tmp_file):
        return tmp_file

    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        ds = None
        try:
            ds = xr.open_dataset(
                dataset_url,
                drop_variables=vars_to_drop,
                decode_times=False
            )
            ds['time'] = decode_time_units(ds['time'])
            ds = ds.sel(lat=lat_range, lon=lon_range)

            ds_day = ds.sel(time=slice(day_start, day_end))
            if ds_day.time.size == 0:
                ds.close()
                return None

            if var_list is not None:
                ds_day = ds_day[var_list]

            if not surface and 'depth' in ds_day.dims:
                ds_day = ds_day.sel(depth=depth_range)

            ds_day.to_netcdf(tmp_file)
            ds.close()
            print(f'  {day_str} OK')
            return tmp_file

        except Exception as e:
            if ds is not None:
                ds.close()
            print(f'  {day_str} attempt {attempt} failed: {e}')
            if attempt < MAX_RETRIES:
                sleep(5)
            else:
                print(f'  {day_str} SKIPPED after {MAX_RETRIES} attempts')
                return None


def download_hycom_gofs31(domain, start_date, end_date, outputDir,
                          var_list=None, depths=[0, 5000], surface=False):
    """
    Downloads HYCOM GOFS 3.1 (GLBy0.08/expt_93.0) data in monthly files.
    Downloads are done day-by-day sequentially, then concatenated into
    monthly YYYY_MM.nc files.

    INPUTS:
    domain     : [lon_min, lon_max, lat_min, lat_max]
    start_date : Start datetime for the download period.
    end_date   : End datetime for the download period.
    outputDir  : Directory to save the downloaded monthly NetCDF files.
    var_list   : List of variable names to download.
                 Default (3-hourly): ['surf_el', 'water_temp', 'salinity', 'water_u', 'water_v']
                 Default (surface):  all surface variables in the dataset
    depths     : [depth_min, depth_max] for subsetting depth (only for 3-hourly 4D variables).
                 Default [0, 5000].
    surface    : False (default) = 3-hourly data, True = hourly surface data.

    OUTPUT:
    Monthly NetCDF files named YYYY_MM.nc in outputDir.
    """

    # OPeNDAP URLs
    # 3-hourly data is a single aggregated dataset
    # Surface data is organized by year: .../sur/{YYYY}
    if surface:
        url_base = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/sur"
    else:
        url = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0"

    # Default variable lists
    if var_list is None:
        if surface:
            var_list = ['ssh', 'qtot', 'emp', 'steric_ssh',
                        'u_barotropic_velocity', 'v_barotropic_velocity',
                        'surface_boundary_layer_thickness', 'mixed_layer_thickness']
        else:
            var_list = ['surf_el', 'water_temp', 'salinity', 'water_u', 'water_v']

    # Variables to drop (auxiliary/coordinate variables we don't need)
    vars_to_drop = ['tau']

    os.makedirs(outputDir, exist_ok=True)

    # Spatial subset slices
    lon_range = slice(domain[0], domain[1])
    lat_range = slice(domain[2], domain[3])
    depth_range = slice(depths[0], depths[1])

    # Loop month by month
    download_date = start_date
    while download_date <= end_date:

        # start and end days of this month
        month_start = datetime(download_date.year, download_date.month, 1)
        day_end = calendar.monthrange(download_date.year, download_date.month)[1]
        month_end = datetime(download_date.year, download_date.month, day_end, 23, 59, 59)

        # output filename matching CMEMS convention
        fname = download_date.strftime('%Y_%m') + '.nc'
        fpath = os.path.join(outputDir, fname)

        print(f'\n{download_date.strftime("%Y-%m")}')

        # skip if file already exists and is valid
        if os.path.exists(fpath):
            try:
                with xr.open_dataset(fpath) as _:
                    print(f'{fname} already exists. Skipping.')
                    download_date = download_date + timedelta(days=32)
                    download_date = datetime(download_date.year, download_date.month, 1)
                    continue
            except Exception:
                print(f'{fname} exists but is invalid. Re-downloading.')
                os.unlink(fpath)

        # For surface data, construct yearly URL
        if surface:
            dataset_url = f"{url_base}/{download_date.year}"
        else:
            dataset_url = url

        # Open dataset with retry logic (lazy load via OPeNDAP)
        MAX_RETRIES = 5
        RETRY_WAIT = 10
        ds = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                ds = xr.open_dataset(
                    dataset_url,
                    drop_variables=vars_to_drop,
                    decode_times=False
                )
                ds['time'] = decode_time_units(ds['time'])
                print(f'[Attempt {attempt}] Dataset opened and times decoded.')
                break
            except Exception as e:
                print(f'[Attempt {attempt}] Failed to open dataset: {e}')
                if ds is not None:
                    ds.close()
                    ds = None
                if attempt < MAX_RETRIES:
                    print(f'Retrying in {RETRY_WAIT} seconds...')
                    sleep(RETRY_WAIT)
                else:
                    raise RuntimeError(
                        f'Failed to open HYCOM GOFS 3.1 dataset after {MAX_RETRIES} attempts.'
                    )

        try:
            # Check data availability for this month using the initial connection
            ds = ds.sel(lat=lat_range, lon=lon_range)
            ds_month_check = ds.sel(time=slice(month_start, month_end))
            if ds_month_check.time.size == 0:
                print(f'No data available for {download_date.strftime("%Y-%m")}. Skipping.')
                ds.close()
                download_date = download_date + timedelta(days=32)
                download_date = datetime(download_date.year, download_date.month, 1)
                continue
            ds.close()

            # Create temp directory for daily files inside outputDir
            tmp_dir = tempfile.mkdtemp(
                prefix=f'.hycom_gofs31_{download_date.strftime("%Y_%m")}_',
                dir=outputDir
            )

            # Build list of days in this month
            days = []
            d = month_start
            while d <= month_end:
                day_start = d
                day_end_dt = datetime(d.year, d.month, d.day, 23, 59, 59)
                days.append((day_start, day_end_dt))
                d = d + timedelta(days=1)

            # Download days sequentially (netCDF4's C library is not thread-safe
            # with OPeNDAP, causing memory corruption when using threads)
            print(f'Downloading {len(days)} days...')
            daily_files = []

            for day_s, day_e in days:
                result = _download_day(
                    dataset_url, day_s, day_e, var_list, depth_range,
                    surface, lon_range, lat_range, vars_to_drop, tmp_dir
                )
                if result is not None:
                    daily_files.append(result)

            # Sort by filename (date order) and concatenate
            daily_files.sort()

            if len(daily_files) == 0:
                print(f'No daily files downloaded for {download_date.strftime("%Y-%m")}. Skipping.')
            else:
                print(f'Concatenating {len(daily_files)} daily files into {fname}...')
                with xr.open_mfdataset(daily_files, combine='by_coords') as ds_combined:
                    ds_combined.to_netcdf(fpath)
                print(f'Saved {fname}')

            # Clean up temp files
            for f in daily_files:
                os.unlink(f)
            os.rmdir(tmp_dir)

        except Exception:
            ds.close()
            raise

        # Advance to next month
        download_date = download_date + timedelta(days=32)
        download_date = datetime(download_date.year, download_date.month, 1)


if __name__ == '__main__':
    run_date = pd.to_datetime('2025-08-22 00:00:00')
    hdays = 0
    fdays = 0
    #domain = [11,36,-39,-25]
    domain = [11,12,-39,-38]
    outputDir = '/home/g.rautenbach/Projects/somisana-croco/DATASETS_CROCOTOOLS/HYCOM/'
    parallel = True
    download_hycom_ops(domain, run_date, hdays, fdays, outputDir, parallel)
      
