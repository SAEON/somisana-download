'''
this serves as a command line interface (CLI) to execute functions from 
within this python repo directly from the command line.
The intended use is to allow python functions to be run from the cli docker image for this repo
So this is the entry point for the docker image (see Dockerfile.cli).
But it's also handy if you want to execute a python function from inside a bash script
The only functions I'm adding here are ones which produce an output e.g. a netcdf file
Feel free to add more functions from the repo as we need them in the cli
'''
import argparse
import sys, os
from datetime import datetime, timedelta
from download_tools.cmems import download_cmems, download_cmems_monthly, download_mercator_ops
from download_tools.gfs import download_gfs_atm
from download_tools.hycom import download_hycom

# functions to help parsing string input to object types needed by python functions
def parse_datetime(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid datetime format. Please use 'YYYY-MM-DD HH:MM:SS'.")

def parse_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid integer value: {value}")

def parse_list(value):
    return [float(x) for x in value.split(',')]

def parse_list_str(value):
    if value is None or value == 'None':
        return None
    else:
        return [x.strip() for x in value.split(',')]
    
def parse_bool(s: str) -> bool:
    try:
        return {'true':True, 'false':False}[s.lower()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'expect true/false, got: {s}')

def main():
    
    parser = argparse.ArgumentParser(description='Command-line interface for selected functions in the somisana-download repo')
    subparsers = parser.add_subparsers(dest='function', help='Select the function to run')

    # just keep adding new subparsers for each new function as we go...

    # -----------------------
    # download_cmems_monthly
    # -----------------------
    parser_download_cmems_monthly = subparsers.add_parser('download_cmems_monthly', 
            help='Generic function to download month by month for any dataset from CMEMS')
    parser_download_cmems_monthly.add_argument('--usrname', required=True, type=str, help='Copernicus username')
    parser_download_cmems_monthly.add_argument('--passwd', required=True, help='Copernicus password')
    parser_download_cmems_monthly.add_argument('--dataset', required=True, help='Copernicus dataset ID')
    parser_download_cmems_monthly.add_argument('--domain', type=parse_list, 
                        default=[23, 34, -37, -31],
                        help='comma separated list of domain extent to download i.e. "lon0,lon1,lat0,lat1"')
    parser_download_cmems_monthly.add_argument('--start_date', required=True, type=parse_datetime, 
                        help='start time in format "YYYY-MM-DD HH:MM:SS"')
    parser_download_cmems_monthly.add_argument('--end_date', required=True, type=parse_datetime, 
                        help='end time in format "YYYY-MM-DD HH:MM:SS"')
    parser_download_cmems_monthly.add_argument('--varList', type=parse_list_str, 
                        default=['so', 'thetao', 'zos', 'uo', 'vo'],
                        help='comma separated list of variables to download e.g. "so,thetao,zos,uo,vo"')
    parser_download_cmems_monthly.add_argument('--depths', type=parse_list, 
                        default=[0.493, 5727.918],
                        help='comma separated list of depth extent to download (positive down). For all depths use "0.493,5727.918"')
    parser_download_cmems_monthly.add_argument('--outputDir', required=True, help='Directory to save files')
    def download_cmems_monthly_handler(args):
        download_cmems_monthly(args.usrname, args.passwd, args.dataset, args.domain, args.start_date,args.end_date,args.varList, args.depths, args.outputDir)
    parser_download_cmems_monthly.set_defaults(func=download_cmems_monthly_handler)

    # ----------------------
    # download_cmems_ops
    # ----------------------
    parser_download_cmems_ops = subparsers.add_parser('download_cmems_ops', 
            help='Download a subset of operational data from CMEMS')
    parser_download_cmems_ops.add_argument('--usrname', required=True, type=str, help='Copernicus username')
    parser_download_cmems_ops.add_argument('--passwd', required=True, help='Copernicus password')
    parser_download_cmems_ops.add_argument('--dataset', required=True, help='Copernicus dataset ID')
    parser_download_cmems_ops.add_argument('--varList', required=True, type=parse_list_str, 
                        help='comma separated list of variables to download e.g. "so,thetao,zos,uo,vo"')
    parser_download_cmems_ops.add_argument('--domain', type=parse_list, 
                        default=[10, 25, -40, -25],
                        help='comma separated list of domain extent to download i.e. "lon0,lon1,lat0,lat1"')
    parser_download_cmems_ops.add_argument('--depths', type=parse_list, 
                        default=[0.493, 5727.918],
                        help='comma separated list of depth extent to download (positive down). For all depths use "0.493,5727.918"')
    parser_download_cmems_ops.add_argument('--run_date', required=True, type=parse_datetime, 
                        help='current time in format "YYYY-MM-DD HH:MM:SS"')
    parser_download_cmems_ops.add_argument('--hdays', required=True, type=float,
                        default=5.,
                        help='hindcast days i.e before run_date')
    parser_download_cmems_ops.add_argument('--fdays', required=True, type=float,
                        default=5.,
                        help='forecast days i.e before run_date')
    parser_download_cmems_ops.add_argument('--outputDir', required=True, help='Directory to save file') 
    parser_download_cmems_ops.add_argument('--outputFile', required=True, help='Output file name') 
    def download_cmems_ops_handler(args):
        start_date = run_date + timedelta(days=-args.hdays)
        end_date = run_date + timedelta(days=args.fdays)
        download_cmems(args.usrname, args.passwd, args.dataset, args.varlist, start_date, end_date, args.domain, args.depths, args.outputDir, args.outputFile)
    parser_download_cmems_ops.set_defaults(func=download_cmems_ops_handler)

    # ----------------------
    # download_mercator_ops
    # ----------------------
    parser_download_mercator_ops = subparsers.add_parser('download_mercator_ops', 
            help='Download a subset of daily MERCATOR 1/12 deg analysis and forecast data from CMEMS')
    parser_download_mercator_ops.add_argument('--usrname', required=True, type=str, help='Copernicus username')
    parser_download_mercator_ops.add_argument('--passwd', required=True, help='Copernicus password')
    parser_download_mercator_ops.add_argument('--domain', type=parse_list, 
                        default=[10, 25, -40, -25],
                        help='comma separated list of domain extent to download i.e. "lon0,lon1,lat0,lat1"')
    parser_download_mercator_ops.add_argument('--run_date', required=True, type=parse_datetime, 
                        help='start time in format "YYYY-MM-DD HH:MM:SS"')
    parser_download_mercator_ops.add_argument('--hdays', required=True, type=float,
                        default=5.,
                        help='hindcast days i.e before run_date')
    parser_download_mercator_ops.add_argument('--fdays', required=True, type=float,
                        default=5.,
                        help='forecast days i.e before run_date')
    parser_download_mercator_ops.add_argument('--outputDir', required=True, help='Directory to save files') 
    def download_mercator_ops_handler(args):
        download_mercator_ops(args.usrname, args.passwd, args.domain, args.run_date,args.hdays, args.fdays,args.outputDir)
    parser_download_mercator_ops.set_defaults(func=download_mercator_ops_handler)
    
    # ------------------
    # download_gfs_atm
    # ------------------
    parser_download_gfs_atm = subparsers.add_parser('download_gfs_atm', 
            help='Download a subset of hourly 0.25 deg gfs atmospheric data from NOAA')
    parser_download_gfs_atm.add_argument('--domain', type=parse_list, 
                        default=[10, 25, -40, -25],
                        help='comma separated list of domain extent to download i.e. "lon0,lon1,lat0,lat1"')
    parser_download_gfs_atm.add_argument('--run_date', required=True, type=parse_datetime, 
                        help='start time in format "YYYY-MM-DD HH:MM:SS"')
    parser_download_gfs_atm.add_argument('--hdays', required=True, type=float,
                        default=5.,
                        help='hindcast days i.e before run_date')
    parser_download_gfs_atm.add_argument('--fdays', required=True, type=float,
                        default=5.,
                        help='forecast days i.e before run_date')
    parser_download_gfs_atm.add_argument('--outputDir', required=True, help='Directory to save files')
    def download_gfs_atm_handler(args):
        download_gfs_atm(args.domain, args.run_date, args.hdays, args.fdays, args.outputDir)
    parser_download_gfs_atm.set_defaults(func=download_gfs_atm_handler) 
    
    # -------------------
    # download_hycom
    # -------------------
    parser_download_hycom = subparsers.add_parser('download_hycom', 
            help='Download a subset of  HYCOM analysis and forecast data using xarray OpenDAP')
    parser_download_hycom.add_argument('--variables',required=False,
                                       default = ['salinity', 'water_temp', 'surf_el', 'water_u', 'water_v'],
                                       help='List of variables to download.')    
    parser_download_hycom.add_argument('--domain', required=False, type=parse_list,
                                       default=[10, 25, -40, -25],
                                       help='comma separated list of domain extent to download i.e. [lon_min,lon_max,lat_min,lat_max]')
    parser_download_hycom.add_argument('--depths', required=False, type=parse_list,
                                       default=[0,5000],
                                       help='Minimum and maximum depths to download. Values must be positive. Default is [0,5000]')
    parser_download_hycom.add_argument('--run_date', required=True, type=parse_datetime,
                                       help='start time in datetime format "YYYY-MM-DD HH:MM:SS"')
    parser_download_hycom.add_argument('--hdays', required=False, type=float,
                                       default=5.,
                                       help='hindcast days i.e before run_date')
    parser_download_hycom.add_argument('--fdays', required=False, type=float, 
                                       default=5.,
                                       help='forecast days i.e before run_date')
    parser_download_hycom.add_argument('--savedir', required=True, 
                                       help='Directory to save files')
    parser_download_hycom.add_argument('--pad',required=False, type=parse_bool,
                                       default=False,
                                       help='Pad all time-dependent variables in the dataset by one timestep at the start and end. At the start, we download and extra day and at the end we copy the last timestep (Default is False). This is used operationally for our forecast models.')
    def download_hycom_handler(args):
        download_hycom(args.variables,args.domain, args.depths, args.run_date, args.hdays, args.fdays, args.savedir, args.pad)
    parser_download_hycom.set_defaults(func=download_hycom_handler)
    
    
    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        print("Please specify a function.")

if __name__ == "__main__":
    main()
