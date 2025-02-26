# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 16:49:16 2024

@author: SHe
"""

#-*- coding: utf-8 -*-
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def Data_code(start_date, end_date):
    
    dates = []
    start = datetime.strptime(str(start_date), "%Y%m%d")
    end = datetime.strptime(str(end_date), "%Y%m%d")
    
    current = start
    while current <= end:
        year, month, day = current.year, current.month, current.day
        dates.append((year, month, day, f"{year}/{month:02d}/{day:02d}/"))
        current += timedelta(days=1)
    
    return dates


def Download(url, save_path, datas):
    download_url = url + datas[-1]
    print(f"访问页面: {datas[-1]}")
    
    response = requests.get(download_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    for link in soup.select('a[href$=".nc"]'):
        name = link['href']
        file_path = os.path.join(save_path, name)
        
        # 如果文件已存在，则跳过下载
        if os.path.exists(file_path):
            print(f"{name} 已存在，跳过下载")
            continue
        
        # 下载文件
        nc = requests.get(download_url + name)        
        with open(file_path, 'wb') as file:
            file.write(nc.content)
            
    # 记录完成的日期
    completed_path = os.path.join('./', f"completed-{Start_date}-{End_date}.txt")
    with open(completed_path, 'a') as log_file:
        log_file.write(datas[-1] + '\n')
    print(datas[-1], '所有文件已完成下载')

        
def download_all_data(url, path, start_date, end_date):
    data_range = Data_code(start_date, end_date)
    
    # 使用 ThreadPoolExecutor 和 tqdm 进行并行下载和进度显示
    with ThreadPoolExecutor(max_workers=48) as executor:
        # tqdm 用于显示总任务的进度条
        list(tqdm(executor.map(lambda day: Download(url, path, day), data_range), total=len(data_range), desc="下载进度"))

Url = 'https://www.ncei.noaa.gov/data/cmorph-high-resolution-global-precipitation-estimates/access/30min/8km/'

Start_date = 20230101
End_date = 20240101
Path = f'./CMORPH-{Start_date}-{End_date}'
if not os.path.exists(Path):
    os.makedirs(Path)
download_all_data(Url, Path, Start_date, End_date)







