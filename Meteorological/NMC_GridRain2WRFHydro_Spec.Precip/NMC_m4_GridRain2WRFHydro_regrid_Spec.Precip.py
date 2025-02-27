"""
auther: shihx2003
create: 2025-02-24 17:46:05
"""

import os
import numpy as np
import xesmf as xe
import xarray as xr

from datetime import datetime, timedelta
import pandas as pd
from joblib import Parallel, delayed

def readm4(file_path):
    with open(file_path, "r") as file:
        header = file.readline().strip()
        file.readline()

        grid_data = []
        for line in file:
            grid_data.extend(map(float, line.strip().split()))
        grid_array = np.array(grid_data).reshape(801, 831)
        grid_array = np.nan_to_num(grid_array, nan=0.0)
        return grid_array
    
def m4name(start_time, end_time):
    file_list = []
    while start_time <= end_time:
        formatted_time_str = start_time.strftime("%Y%m%d%H")
        file_name = formatted_time_str + '.m4'
        file_list.append(file_name)
        start_time += timedelta(hours=1)

    return file_list

def valuecheck(m4_value):
    sorted_m4_value = np.sort(m4_value, axis=None)[::-1]
    print(f"Sorted m4_value: {sorted_m4_value[0:15]}")

def process_m4_file(i, m4_info):
    m4_name = m4_info[0][i]
    time = m4_info[1][i]
    m4_path = os.path.join(m4path, m4_name)
    m4_value = readm4(m4_path)
    m4_ds = xr.Dataset(
        {
            "precip": (["time", "lat", "lon"], m4_value[np.newaxis, :, :])
        },
        coords={
            "time": [time],
            "lon": lon,
            "lat": lat
        }
    )
    m4_ds["precip"].attrs["units"] = "mm/h"
    m4_ds["precip"].attrs["description"] = "diamond 4 NMC_01H_GridRain_(Valid_1H)"

    return m4_ds

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

start = datetime(2023, 7, 1, 0)
end = datetime(2023, 9, 15, 23)
m4path = r"./2023"
lon_min = 111.7
lon_max = 120.0
lat_min = 35.0
lat_max = 43.0
grid_resolution = 0.01
lon = (np.arange(lon_min*100, (lon_max + grid_resolution)*100, grid_resolution*100)) / 100
lat = (np.arange(lat_min*100, (lat_max + grid_resolution)*100, grid_resolution*100)) / 100
m4_info = m4name(start, end)
time_coords = pd.date_range(start, end, freq="h")

geo_sm = "./GEOGRID_LDASOUT_Spatial_Metadata.nc"
geo_em = "./geo_em.d03.nc"
geo_sm_ds = xr.open_dataset(geo_sm, autoclose=True)
geo_em_ds = xr.open_dataset(geo_em)
geo_em_ds = geo_em_ds.rename({'XLONG_M': 'lon', 'XLAT_M': 'lat'})
geo_em_ds['lat'] = geo_em_ds['lat'].sel(Time=0, drop=True)
geo_em_ds['lon'] = geo_em_ds['lon'].sel(Time=0, drop=True)


npart = (len(m4_info) // 720) + 1
div_m4_info = np.array_split(m4_info, npart)
div_time_coords = np.array_split(time_coords, npart)

print("Total number of parts: ", len(div_m4_info))
for i in range(len(div_m4_info)):
    print("Processing part: ", i)
    datasets = []
    part_len = len(div_m4_info[i])
    div_m4_info = [div_m4_info[i], div_time_coords[i]]

    results = Parallel(n_jobs=-1)(delayed(process_m4_file)(index, div_m4_info) for index in range(part_len))

    datasets.extend(results)
    forcing_ds = xr.concat(datasets, dim="time")

    forcing_ds['precip_rate'] = forcing_ds.precip / (60.0*60.0)
    forcing_regrid = precip_regrid(forcing_ds, geo_em_ds, geo_sm_ds)
    save_PRECIP_FORCING(forcing_regrid)




