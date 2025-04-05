# -*- encoding: utf-8 -*-
'''
@File    :   analysize.py
@Create  :   2025-04-05 13:00:48
@Author  :   shihx2003
@Version :   1.0
@Contact :   shihx2003@outlook.com
'''

# here put the import lib

import os
import yaml
import numpy as np
import wrfhydrofrxst as whf
import pandas as pd

No = 20190804
station = {'1':'Fuping'}

default = ['./result/pre_10000_Fuping_20190804.txt']
default_sim = whf.Readfrxst_pts_out(default, station)
default_sim = whf.ConvertTimeZone(default_sim, 'UTC', 'Asia/Shanghai')

# Read observation data
Fuping_info = r"F:\水文年鉴\海河流域-大清河水系-洪水\FloodEvents\0-Fuping_flood_info.xlsx"
Fuping_info = pd.read_excel(Fuping_info, sheet_name='Sheet1')
flood_info = Fuping_info[Fuping_info['No'] == No]
start_time = flood_info.iloc[0]['start_date']
end_time = flood_info.iloc[0]['end_date']
obs = f"F:\\水文年鉴\\海河流域-大清河水系-洪水\\FloodEvents\\Fuping_{No}.xlsx"
obs = pd.read_excel(obs, sheet_name='Sheet1')
obs = obs.rename(columns={'Q': 'obs'})

# Read default simulation data
default = ['./result/pre_10000_Fuping_20190804.txt']
default_sim = whf.Readfrxst_pts_out(default, station)
default_sim = whf.ConvertTimeZone(default_sim, 'UTC', 'Asia/Shanghai')
default_sim = default_sim[(default_sim['Date'] >= start_time) & (default_sim['Date'] <= end_time)]
default_sim = default_sim.rename(columns={'Fuping_pre_10000_Fuping_20190804': 'default'})

# Read simulation data
# Read configuration from YAML file
yaml_path = 'run_jobs.yaml'  # Update with your actual YAML file path
with open(yaml_path, 'r') as file:
    run_jobs = yaml.safe_load(file)

# Read simulation data from paths specified in the YAML
# percent = ['(0%)', '(25%)', '(50%)', '(75%)', '(100%)']
job_num = 10001
group_file = []
group_params = []
for job_num in range(10001, 10116):
    job_id = f'pre_{job_num}'
    job_path = f'./result/{job_id}_Fuping_20190804.txt'
    set_params = run_jobs[job_id]['set_params']
    group_file.append(job_path)
    group_params.append(set_params)

    if job_num % 5 == 0:
        rename_columns = {}
        for i in range(len(group_params)):
            param_str = ''.join([f"{k}={v}" for k, v in group_params[i].items()]) + f'({i*25}%)'
            rename_columns['Fuping_' + os.path.splitext(os.path.basename(group_file[i]))[0]] = param_str

        filename = list(group_params[0].keys())[0]
        sim = whf.Readfrxst_pts_out(group_file, station)
        sim = whf.ConvertTimeZone(sim, 'UTC', 'Asia/Shanghai')
        sim = sim[(sim['Date'] >= start_time) & (sim['Date'] <= end_time)]
        sim = sim.rename(columns=rename_columns)
        sim = pd.merge(sim, default_sim, on='Date', how='outer')
        
        whf.DrawStreamFlow(obs, sim, filename)

        # clear the lists for the next iteration
        group_file = []
        group_params = []
