import os
import zipfile
from datetime import datetime, timedelta

def generate_date(year):
    file_list = []
    start_time = datetime(year, 1, 1, 0)
    while start_time.year == year:
        formatted_time_str = start_time.strftime("%Y%m%d%H")
        file_name = str(year) + '/' + formatted_time_str + '.m4'
        file_list.append(file_name)
        start_time += timedelta(hours=1)

    return file_list

def check_missing_times(file_own, file_all):
    missing_times = [time for time in file_all if time not in file_own]
    return missing_times

years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2018, 2019, 2022, 2023]
for year in years:
    zip_file_path = os.path.join(r'F:\ART_1km', f'{year}.zip')
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        print(zip_ref.namelist()[0])
        file_own = zip_ref.namelist()[1:]
        file_all = generate_date(year)
        missing_files = check_missing_times(file_own, file_all)

        if len(missing_files) == 0:
            print("All files are present")
        else:
            print(f"Missing files lenth: {len(missing_files)}")
            missing_times_file = os.path.join(r'F:\ART_1km\MISS', f'missing_times_{year}.txt')
            with open(missing_times_file, 'w') as f:
                for missing_file in missing_files:
                    missing_time = missing_file.replace('.', '/').split('/')[1]
                    f.write(missing_time + '\n')
