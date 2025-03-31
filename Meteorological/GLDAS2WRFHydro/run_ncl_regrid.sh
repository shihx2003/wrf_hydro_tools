#!/bin/bash
nohup ncl 'srcFileName="GLDAS*nc4"' 'dstGridName="geo_em.d02.nc"' GLDAS2WRFHydro_regrid.ncl > forcing_2010_GLDAS2WRFHydro_regrid.log 2>&1 &
