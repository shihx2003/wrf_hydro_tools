# -*- encoding: utf-8 -*-
'''
@File    :   wrfhydrofrxst.py
@Create  :   2025-03-19 19:45:27
@Author  :   shihx2003
@Version :   1.0
@Contact :   shihx2003@outlook.com
'''

# here put the import lib

import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import matplotlib.pyplot as plt

def Readfrxst_pts_out(frxst_paths, station, exta=''):
    """
    1.input frxst_pts_out.txt path(str), input_station(dict,{'1':station1, ...}).
    
    2.split flow values of different stations according No.
    
    3.rename columns by No. and station name.
    
    4.output dataframe with columns like ['date', 'station1', ...]
    
    """
    if isinstance(frxst_paths, str):
        frxst_paths = [frxst_paths]

    floods = []
    for frxst in frxst_paths:
        frxst_name = os.path.splitext(os.path.basename(frxst))[0]
        frxst_station = station.copy()

        for key, value in frxst_station.items():
            if exta:
                frxst_station[key] = value + '_' + str(exta) + '_' + frxst_name
            else:
                frxst_station[key] = value + '_' + frxst_name

        flow_values = {}
        with open(frxst, 'r') as file:
            lines = file.readlines()
            txt_content = [line.strip().split(',') for line in lines]
            
        datas = [datetime.strptime(item[1], "%Y-%m-%d %H:%M:%S") for item in txt_content]
        
        flow_values['Date'] = list(dict.fromkeys(datas))
        
        for item in txt_content:
            station_no = str(item[2].strip())
            flow_value = float(item[5].strip())
            flow_values[station_no] = flow_values.get(station_no, []) + [flow_value]    
        flow_df = pd.DataFrame(flow_values)
        flow_df = flow_df.rename(columns=frxst_station)
        floods.append(flow_df)

    if len(floods) > 1:
        result = floods[0]
        for df in floods[1:]:
            result = pd.merge(result, df, on='Date', how='outer')
    else:
        result = floods[0]

    return result

def ConvertTimeZone(input_df, input_origin="UTC",input_traget="Asia/Shanghai"):
    """
    1.Used for convert time zone of date in dataframe 
    
    2.input(df with cloumn named "Date"), output(df)

    """
    
    out_df = input_df.copy()
    out_df['Date'] = pd.to_datetime(out_df['Date'])

    origin_timezone = ZoneInfo(input_origin)
    traget_timezone = ZoneInfo(input_traget)

    out_df['Date'] = out_df['Date'].apply(
        lambda x: x.replace(tzinfo=origin_timezone).astimezone(traget_timezone).strftime('%Y-%m-%d %H:%M:%S')
    )

    out_df['Date'] = pd.to_datetime(out_df['Date'], format='%Y-%m-%d %H:%M:%S')
    
    return out_df
        
def CalDailyAvg(input_df):
    """
    1.Used for calculate daily average value
    
    2.input(df with cloumn named "date"), output(df)
    
    """
    temp_df = input_df.copy()
    temp_df['Date'] = pd.to_datetime(temp_df['Date'])
    temp_df.set_index('Date', inplace=True)
    out_df = temp_df.resample('D').mean()
    out_df = out_df.reset_index()
    return out_df

def DrawStreamFlow(obs, sim, filename, **kwargs):
    """
    Used for draw streamflow chart, input obs and sim dataframe.

    Parameters
    ----------
    obs : DataFrame
        Observed data with columns ['Date', 'obs']
    sim : DataFrame
        Simulated data with columns ['Date', 'sim1', 'sim2', ...]
    filename : str
        File name for saving the chart.
    kwargs : dict
        Additional parameters:
            - out_dir : str, output directory for saving charts (default: './charts')
            - mark : str, file name mark (default: '')
            - date_name : str, name of the date column (default: 'Date')
            - obs_name : str, name of the observed data column (default: 'obs')
            - fig_size : tuple, figure size (default: (18, 6))
    """
    out_dir = kwargs.get('out_dir', './charts')
    mark = kwargs.get('mark', '')
    date_name = kwargs.get('datename', 'Date')
    obs_name = kwargs.get('obsname', 'obs')
    fig_size = kwargs.get('figsize', (18, 6))

    sim_columns = sim.columns.tolist()[1:]
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    
    plt.figure(figsize=fig_size, dpi=200)
    plt.plot(obs[date_name], obs[obs_name], label=obs_name, color='black', linestyle='-')

    for column in sim_columns:
        plt.plot(sim[date_name], sim[column], label=column)
    
    ymax = max(sim[sim_columns].max().max(), obs[obs_name].max()) * 1.2

    plt.ylabel('Flow Values')
    plt.grid(True)
    ax = plt.gca()
    ax.set_ylim(0, ymax)
    plt.xlabel(date_name)
    plt.legend()
    plt.title(f'Flow Values Over Time-{mark}')
    plt.savefig(os.path.join(out_dir, f'{filename} {mark}.png'))
    print(f'Saved {filename} {mark}.png')

if __name__ == '__main__':
    
    station = {'1':'Zijinguan',
            '2':'Zhangfang',
            '3':'Manshuihe',
            '4':'Beihedian',
            }
    frxst_path = r"./new.txt"

    f = Readfrxst_pts_out(frxst_path, station)
    ft = ConvertTimeZone(f)
    fta = CalDailyAvg(ft)
    ft.to_csv("Haihe_4subbasin-1H.csv", index=False)
    fta.to_csv("Haihe_4subbasin-1Davg.csv", index=False)
    DrawStreamFlow(ft)








