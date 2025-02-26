#!/bin/bash

echo "######################################### All Start ##########################################"
year=2023
cmorph_input=/home/shihx/Data/CMORPH_V1.0_ADJ_8km-30min/CMORPH_test
cmorph_output=/home/shihx/Data/CMORPH_V1.0_ADJ_8km-30min/CMORPH_out
# Clear old files
mkdir ${cmorph_output}
echo "#################################### Year = ${year} #####################################"
for month in {07..07}; do
	echo "############################### Start Month = ${month} ################################"
	for day in {01..03}; do
		# Check current_date ture or false 
		if date -d "${year}-${month}-${day}" >/dev/null 2>&1; then
			current_date=$(date -d "${year}-${month}-${day}" +"%Y%m%d")
			start_date=$(date -d "${year}-${month}-${day}" +"%Y-%m-%d")
			next_date=$(date -d "${year}-${month}-${day} +1 day" +"%Y%m%d")
			echo "######################### ${current_date} Start ############################"
			
			for hour in {00..23}; do
				current_hour="${current_date}${hour}"
				# sum
				cdo timselmean,2 ${cmorph_input}/CMORPH_V1.0_ADJ_8km-30min_${current_hour}.nc ${cmorph_output}/CMORPH_V1.0_ADJ_8km-1h_${current_hour}.nc
				wait
				
			done
			echo "######################### ${current_date} Finished #########################"
		fi
	done
	echo "###############################  Month = ${month} Finished ################################"
done
# clear temp
echo "######################################## All Finished ########################################"
