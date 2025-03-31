#!/bin/bash
nohup bash interp_GLDAS_3Hto1H.sh 2010-02-28 2010-03-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100228_20100305.log 2>&1 &
nohup bash interp_GLDAS_3Hto1H.sh 2010-03-06 2010-04-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100306_20100405.log 2>&1 &
nohup bash interp_GLDAS_3Hto1H.sh 2010-04-06 2010-05-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100406_20100505.log 2>&1 &
nohup bash interp_GLDAS_3Hto1H.sh 2010-05-06 2010-06-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100506_20100605.log 2>&1 &
nohup bash interp_GLDAS_3Hto1H.sh 2010-06-06 2010-07-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100606_20100705.log 2>&1 &
nohup bash interp_GLDAS_3Hto1H.sh 2010-07-06 2010-08-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100706_20100805.log 2>&1 &
nohup bash interp_GLDAS_3Hto1H.sh 2010-08-06 2010-09-05 /home/Shihuaixuan/Data/smt_GLDAS/regrid_GLDAS_2010 > forcing_interp_20100806_20100905.log 2>&1 &


