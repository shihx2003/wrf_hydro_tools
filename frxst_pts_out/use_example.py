# -*- encoding: utf-8 -*-
'''
@File    :   test.py
@Create    :   2025-03-19 18:37:51
@Author  :   shihx2003
@Version :   1.0
@Contact :   shihx2003@outlook.com
'''

# here put the import lib

import os
import wrfhydrofrxst as whf
import pandas as pd

No = 20190804
station = {'1':'Fuping'}
frxst_paths = ['./100m.txt','./100m.txt', './200m.txt']

sim = whf.Readfrxst_pts_out(frxst_paths, station)
sim = whf.ConvertTimeZone(sim, 'UTC', 'Asia/Shanghai')

Fuping_info = r"F:\水文年鉴\海河流域-大清河水系-洪水\FloodEvents\0-Fuping_flood_info.xlsx"
Fuping_info = pd.read_excel(Fuping_info, sheet_name='Sheet1')
flood_info = Fuping_info[Fuping_info['No'] == No]
start_time = flood_info.iloc[0]['start_date']
end_time = flood_info.iloc[0]['end_date']

obs = f"F:\\水文年鉴\\海河流域-大清河水系-洪水\\FloodEvents\\Fuping_{No}.xlsx"
obs = pd.read_excel(obs, sheet_name='Sheet1')
sim = sim[(sim['Date'] >= start_time) & (sim['Date'] <= end_time)]

Q = pd.merge(sim, obs, on='Date', how='outer')

whf.DrawStreamFlow(Q, 'Fuping_'+str(No))
print(Q)
