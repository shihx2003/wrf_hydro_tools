"""
auther: shihx2003
create: 2025-03-11 14:20:05
"""

import os
import zipfile
import numpy as np
import pandas as pd
import xesmf as xe
import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from joblib import Parallel, delayed

def readzip(zip_path, file_name):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with zip_ref.open(file_name) as file:
            content = file.read()
            return content

def readgrd(zip_path, grd_path):
    nx, ny = 700, 440
    content = readzip(zip_path, grd_path)
    grid_data = np.frombuffer(content, dtype=np.float32)
    grid_data = grid_data.reshape(2, ny, nx)
    grid_data=np.maximum(grid_data[0, :, :], 0)

    return grid_data

def grdname(start_time, end_time):
    file_list = []
    while start_time <= end_time:
        formatted_time_str = start_time.strftime("%Y%m%d%H")
        file_name = 'SURF_CLI_CHN_MERGE_CMP_PRE_HOUR_GRID_0.10-' + formatted_time_str + '.grd'
        file_list.append(file_name)
        start_time += timedelta(hours=1)

    return file_list

def process_grd_file(i, grd_info):
    grd_name = grd_info[0][i]
    time = grd_info[1][i]

    global zip_path, in_grd_path, lon, lat

    grd_path = in_grd_path + "/" + grd_name
    grd_value = readgrd(zip_path, grd_path)
    grd_ds = xr.Dataset(
        {
            "precip": (["time", "lat", "lon"], grd_value[np.newaxis, :, :])
        },
        coords={
            "time": [time],
            "lon": lon,
            "lat": lat
        }
    )
    grd_ds["precip"].attrs["units"] = "mm/h"
    grd_ds["precip"].attrs["description"] = "China Hourly Merged Precipitation Analysis(CMORPH) Data"

    return grd_ds

def precip_regrid(forcing, geo_em, geo_sm):

    regridder = xe.Regridder(forcing, geo_em, 'bilinear')
    forcing_regrid = regridder(forcing.precip)
    forcing_regrid.coords['west_east'] = geo_sm.x.values
    forcing_regrid.coords['south_north'] = geo_sm.y.values
    forcing_regrid = forcing_regrid.rename({'west_east': 'x', 'south_north': 'y'})
    forcing_regrid.attrs['esri_pe_string'] = geo_sm.crs.attrs['esri_pe_string']
    forcing_regrid.attrs['units'] = 'mm/s'

    return forcing_regrid

def save_PRECIP_FORCING(forcing_regrid):
    dates = pd.to_datetime(forcing_regrid.time.values)
    for i in range(dates.size):
        str = dates[i].strftime('%Y%m%d%H')
        precip_forcing = forcing_regrid.isel(time=[i]).to_dataset(name="precip_rate")
        precip_forcing.to_netcdf(f"./output_files/{str}00.PRECIP_FORCING.nc")

start = datetime(2020, 7, 15, 0)
end = datetime(2020, 8, 10, 23)
zip_path = r"F:\CMORPH_Station\2020.zip"
in_grd_path = "2020"
lon_min = 70.05
lon_max = 139.95
lat_min = 15.05
lat_max = 58.95
grid_resolution = 0.1
lon = (np.arange(lon_min*100, (lon_max + grid_resolution)*100, grid_resolution*100)) / 100
lat = (np.arange(lat_min*100, (lat_max + grid_resolution)*100, grid_resolution*100)) / 100

grd_info = grdname(start, end)
time_coords = pd.date_range(start, end, freq="h")

geo_sm = "./GEOGRID_LDASOUT_Spatial_Metadata.nc"
geo_em = "./geo_em.d03.nc"
geo_sm_ds = xr.open_dataset(geo_sm, autoclose=True)
geo_em_ds = xr.open_dataset(geo_em)
geo_em_ds = geo_em_ds.rename({'XLONG_M': 'lon', 'XLAT_M': 'lat'})
geo_em_ds['lat'] = geo_em_ds['lat'].sel(Time=0, drop=True)
geo_em_ds['lon'] = geo_em_ds['lon'].sel(Time=0, drop=True)

npart = (len(grd_info) // 720) + 1
div_grd_info = np.array_split(grd_info, npart)
div_time_coords = np.array_split(time_coords, npart)

print("Total number of parts: ", npart)
for i in range(npart):
    print("Processing part: ", i + 1)
    datasets = []
    c_grd_info = [div_grd_info[i], div_time_coords[i]]

    results = Parallel(n_jobs=-1)(delayed(process_grd_file)(index, c_grd_info) for index in range(len(div_grd_info[i])))

    datasets.extend(results)
    forcing_ds = xr.concat(datasets, dim="time")

    forcing_ds['precip_rate'] = forcing_ds.precip / (60.0*60.0)
    forcing_regrid = precip_regrid(forcing_ds, geo_em_ds, geo_sm_ds)
    save_PRECIP_FORCING(forcing_regrid)
