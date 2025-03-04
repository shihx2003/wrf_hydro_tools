#!/bin/bash

echo "######################################### All Start ##########################################"
year=2023
gladas_files=/public/home/Shihuaixuan/Data/GLDAS
# Clear old files
mkdir temp_work
mkdir input_files
mkdir input_files
rm -r ./temp_work/*
#rm -r ./input_files/*
echo "#################################### Year = ${year} #####################################"
for month in {07..08}; do
	echo "############################### Start Month = ${month} ################################"
	for day in {01..31}; do
		# Check current_date ture or false 
		if date -d "${year}-${month}-${day}" >/dev/null 2>&1; then
			current_date=$(date -d "${year}-${month}-${day}" +"%Y%m%d")
			start_date=$(date -d "${year}-${month}-${day}" +"%Y-%m-%d")
			next_date=$(date -d "${year}-${month}-${day} +1 day" +"%Y%m%d")
			echo "######################### ${current_date} Start ############################"
			
			# cdo process
			echo "Start Merge ${start_date}"
			cdo mergetime ${gladas_files}/GLDAS_NOAH025_3H.A${current_date}.*.021.nc4 ${gladas_files}/GLDAS_NOAH025_3H.A${next_date}.0000.021.nc4 ./temp_work/merged_3h_${current_date}.nc4
			wait
			echo "Merge Finished"
			
			echo "Start Interp ${start_date}"
			cdo inttime,${start_date},00:00:00,1hour ./temp_work/merged_3h_${current_date}.nc4 ./temp_work/interp_1h_${current_date}.nc4
			wait
			echo "Interp Finished"
			
			# cat time steps
			echo "Start Split ${start_date}"
			time_steps=$(cdo showtimestamp ./temp_work/interp_1h_${current_date}.nc4 | tr ' ' '\n')
			for time_step in $time_steps; do	
				# crear file names
				date_str=$(date -d "$time_step" +"%Y%m%d.%H00")
				output_file="./input_files/GLDAS_NOAH025_3H.A${date_str}.021.nc4"
				cdo seldate,$time_step ./temp_work/interp_1h_${current_date}.nc4 ${output_file}
				wait
			done
			echo "Split Finished"
			
			# clear temp_files
			echo "Start Clear ${current_date} temp_files"
			rm -r ./temp_work/*
			wait
			echo "Clear temp_files Finished"
			echo "######################### ${current_date} Finished #########################"
		fi
	done
	echo "###############################  Month = ${month} Finished ################################"
done
# clear temp
rm -r temp_work
echo "temp_work Cleard"
echo "######################################## All Finished ########################################"
