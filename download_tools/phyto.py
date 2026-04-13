import os
import rioxarray


def download_csir_flags(run_date,data_dir,save_dir):
    """
    Function to convert CSIR phytoplankton flags from GeoTIF to NetCDF.

    Parameters
    ----------
    run_date : str
        Date string in format YYYYMMDD (e.g., '20260410')
    data_dir : str
        Path/URL to where TIF file is stored.
    save_dir : str
        Directory to save the NetCDF file

    Returns
    -------
    str
        Path to the saved NetCDF file
    """

    # Ensure output directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Build file paths
    tif_file = f"phyto_south_africa_{run_date}.tif"
    nc_file = f"phyto_south_africa_{run_date}.nc"
    
    tif_path = os.path.join(data_dir, tif_file)
    nc_path = os.path.join(save_dir, nc_file)

    try:
        print(f"Opening tif file: {tif_path}")
        da = rioxarray.open_rasterio(tif_path)

        print("Converting to xarray dataset...")
        ds = da.to_dataset(name="phytoplankton")

        print(f"Writing NetCDF to: {nc_path}")
        ds.to_netcdf(nc_path)

        print("All Done")
        return nc_path

    except Exception as e:
        print(f"Error processing {run_date}: {e}")
        raise
