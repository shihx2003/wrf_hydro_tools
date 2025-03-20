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

def DrawStreamFlow(input_data, info=''):
    """
    1.draw for preview
    2.input(df with cloumn named "Date")
    
    """
    columns = input_data.columns.tolist()[1:]
    output_folder = 'charts'
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    
    plt.figure(figsize=(18, 6), dpi=200)

    for column in columns:
        plt.plot(input_data['Date'], input_data[column], label=column)
    
    plt.ylabel('Flow Values')
    plt.grid(True)
    ax = plt.gca()
    ax.set_ylim(0, 1.2 * input_data[columns].max().max())
    plt.xlabel('Date')
    plt.title(f'Flow Values Over Time-{info}')
    plt.legend()
    output_file = os.path.join(output_folder, f'{info}.png')
    plt.savefig(output_file)
    plt.show()
    print(f'Saved {info}.png')

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








