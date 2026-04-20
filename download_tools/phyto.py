import rioxarray

def download_phytoplankton_flags(file_in,file_out):
    """
    Function to download CSIR phytoplankton flags and save as NetCDF file.

    Parameters
    ----------
    file_in : str
        Path/URL to phytoplankton flags Geotif file
    file_out : str
        Directory and file name of the output NetCDF file
    """
    # Build file path
    try:
        print(f"Opening tif file: {file_in}")
        da = rioxarray.open_rasterio(file_in)

        print("Converting to xarray dataset...")
        ds = da.to_dataset(name="phytoplankton")

        print(f"Writing NetCDF to: {file_out}")
        ds.to_netcdf(file_out)

        print("All Done")
    except Exception as e:
        print(f"Error processing {file_in}: {e}")
        raise
