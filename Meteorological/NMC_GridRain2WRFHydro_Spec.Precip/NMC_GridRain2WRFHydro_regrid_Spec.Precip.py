"""
auther: shihx2003
create: 2025-02-24 17:46:05
"""

import pandas as pd
import xarray as xr
import xesmf as xe

forcing = "./NMC_01H_GridRain_combined_20230701-20230815.nc"
geo_sm = "./GEOGRID_LDASOUT_Spatial_Metadata.nc"
geo_em = "./geo_em.d03.nc"

ds_sm = xr.open_dataset(geo_sm, autoclose=True)
geo_em_ds = xr.open_dataset(geo_em)
forcing_ds = xr.open_dataset(forcing)

geo_em_ds = geo_em_ds.rename({'XLONG_M': 'lon', 'XLAT_M': 'lat'})
geo_em_ds['lat'] = geo_em_ds['lat'].sel(Time=0, drop=True)
geo_em_ds['lon'] = geo_em_ds['lon'].sel(Time=0, drop=True)

forcing_ds['precip_rate'] = forcing_ds.precip / (60.0*60.0)

regridder = xe.Regridder(forcing_ds, geo_em_ds, 'bilinear')
forcing_regrid = regridder(forcing_ds.precip_rate)

forcing_regrid.coords['west_east'] = ds_sm.x.values
forcing_regrid.coords['south_north'] = ds_sm.y.values
forcing_regrid = forcing_regrid.rename({'west_east': 'x', 'south_north': 'y'})
forcing_regrid.attrs['esri_pe_string'] = ds_sm.crs.attrs['esri_pe_string']
forcing_regrid.attrs['units'] = 'mm/s'

dates = pd.to_datetime(forcing_regrid.time.values)
for i in range(dates.size):
    str = dates[i].strftime('%Y%m%d%H')
    print(str)
    precip_forcing = forcing_regrid.isel(time=[i]).to_dataset(name="precip_rate")
    precip_forcing.to_netcdf(f"./output_files/{str}00.PRECIP_FORCING.nc")

