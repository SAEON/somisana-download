import rioxarray
import pandas as pd
import os

def download_phytoplankton_flags(date,output_dir):
    """
    Function to download CSIR phytoplankton flags and save as NetCDF file. CSIR 
    disseminates the data freely using the link below: 
        https://www.ocims.gov.za/data/s3olci/s3-phytoplankton-south_africa

    Parameters
    ----------
    date : pandas datetime - datetime.datetime(YYYY,MM,DD)
        Date of the phytoplankton flags to download from CSIR. 
    dir_out : str
        Directory to save downloaded phytoplankton flags to as a NetCDF file.
    """
    # Build file path
    url = "https://www.ocims.gov.za/data/s3olci/s3-phytoplankton-south_africa"
    file_in = os.path.join(url, f"phyto_south_africa_{date.strftime('%Y%m%d')}.tif" )
    try:
        print(f"\nOpening tif file: {file_in}")
        da = rioxarray.open_rasterio(file_in)

        print("\nConverting to xarray dataset...")
        ds = da.to_dataset(name="phytoplankton")

        file_out = os.path.join(output_dir, f"PHYTO_{date.strftime('%Y%m%d_%H')}.nc")
        print(f"\nWriting NetCDF to: {file_out}")
        ds.to_netcdf(file_out)

        print("\nAll Done")
    except Exception as e:
        print(f"Error processing {file_in}: {e}")
        raise
        
        
if __name__ == '__main__':
    date = pd.to_datetime('2025-08-22 00:00:00')
    output_dir = '/home/g.rautenbach/Data/OLCHI'
    download_phytoplankton_flags(date, output_dir)

