"""
auther: shihx2003
create: 2025-02-24 17:46:05
2023-07-01
50days
"""
import os
import numpy as np
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

start = datetime(2023, 7, 1, 0)
end = datetime(2023, 8, 15, 23)
path = r"./2023"
lon_min = 111.7
lon_max = 120.0
lat_min = 35.0
lat_max = 43.0
grid_resolution = 0.01
lon = np.arange(lon_min*100, (lon_max + grid_resolution)*100, grid_resolution*100)
lat = np.arange(lat_min*100, (lat_max + grid_resolution)*100, grid_resolution*100)
lon = lon / 100
lat = lat / 100
sum_value = np.zeros((len(lat), len(lon)))
m4_names = m4name(start, end)
time_coords = pd.date_range(start, end, freq="h")
colletion = []
datasets = []
def process_file(i, name):
    print(name)
    m4_path = os.path.join(path, name)
    m4_value = readm4(m4_path)
    sum_value = m4_value  # Initialize sum_value for each file
    colletion.append((name, np.max(m4_value), np.max(sum_value)))

    m4_ds = xr.Dataset(
        {
            "precip": (["time", "lat", "lon"], m4_value[np.newaxis, :, :])
        },
        coords={
            "time": [time_coords[i]],
            "lon": lon,
            "lat": lat
        }
    )
    m4_ds["precip"].attrs["units"] = "unknown"
    m4_ds["precip"].attrs["description"] = "diamond 4 NMC_01H_GridRain_(Valid_1H)"

    m4_ds.to_netcdf(f"./input_files/NMC_01H_GridRain_{name[:-3]}.nc")
    return m4_ds

results = Parallel(n_jobs=-1)(delayed(process_file)(i, name) for i, name in enumerate(m4_names))
datasets.extend(results)

all_ds = xr.concat(datasets, dim="time")
print(all_ds)
all_ds.to_netcdf(f"./NMC_01H_GridRain_combined_{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}.nc")

df = pd.DataFrame(colletion, columns=["filename", "max_value", "max_value_sum"])
df.to_csv("./collection.csv", index=False)