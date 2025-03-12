#!/bin/bash
# Author: shihx
# Create: 2025-03-12 15:34:57
# Set the start and end dates
start_date="2020-07-15"  # Modify this to your desired start date (YYYY-MM-DD)
end_date="2020-08-10"    # Modify this to your desired end date (YYYY-MM-DD)

echo "###################################### All Start ##########################################"
gladas_files="/public/home/Shihuaixuan/Data/GLDAS_NOAH025_3H.2.1/GLDAS_NOAH025_3H.2.1"

# Create working directories
mkdir -p temp_work input_files
rm -rf ./temp_work/*  # Ensure the temp_work directory is empty

# Iterate through the specified date range
current_date="$start_date"
while [[ "$current_date" < "$end_date" ]] || [[ "$current_date" == "$end_date" ]]; do
    # Extract year, month, and day
    year=$(date -d "$current_date" +"%Y")
    month=$(date -d "$current_date" +"%m")
    day=$(date -d "$current_date" +"%d")
    
    echo "######################### Processing Date: $current_date ############################"

    # Calculate the next date
    next_date=$(date -d "$current_date +1 day" +"%Y%m%d")

    # Format the current date
    current_date_str=$(date -d "$current_date" +"%Y%m%d")
    
    echo "Start Merging Files for ${current_date}..."
    cdo mergetime ${gladas_files}/${year}/GLDAS_NOAH025_3H.A${current_date_str}.*.021.nc4 \
                  ${gladas_files}/${year}/GLDAS_NOAH025_3H.A${next_date}.0000.021.nc4 \
                  ./temp_work/merged_3h_${current_date_str}.nc4
    if [[ $? -ne 0 ]]; then
        echo "Error: Merging failed for ${current_date}. Skipping..."
        current_date=$(date -d "$current_date +1 day" +"%Y-%m-%d")
        continue
    fi
    echo "Merging Completed"

    echo "Start Interpolating for ${current_date}..."
    cdo inttime,${current_date},00:00:00,1hour ./temp_work/merged_3h_${current_date_str}.nc4 \
                                                ./temp_work/interp_1h_${current_date_str}.nc4
    if [[ $? -ne 0 ]]; then
        echo "Error: Interpolation failed for ${current_date}. Skipping..."
        current_date=$(date -d "$current_date +1 day" +"%Y-%m-%d")
        continue
    fi
    echo "Interpolation Completed"

    echo "Start Splitting Time Steps for ${current_date}..."
    time_steps=$(cdo showtimestamp ./temp_work/interp_1h_${current_date_str}.nc4 | tr ' ' '\n')
    for time_step in $time_steps; do
        # Generate the output filename
        date_str=$(date -d "$time_step" +"%Y%m%d.%H00")
        output_file="./input_files/GLDAS_NOAH025_3H.A${date_str}.021.nc4"
        
        cdo seldate,$time_step ./temp_work/interp_1h_${current_date_str}.nc4 ${output_file}
        if [[ $? -ne 0 ]]; then
            echo "Error: Splitting failed for timestamp ${time_step}. Skipping..."
            continue
        fi
    done
    echo "Splitting Completed"

    # Clean up temporary files
    echo "Cleaning up temporary files for ${current_date}..."
    rm -rf ./temp_work/*
    echo "Cleanup Completed"

    # Move to the next date
    current_date=$(date -d "$current_date +1 day" +"%Y-%m-%d")
    echo "######################### ${current_date} Processing Completed #########################"
done

# Final cleanup of temporary directory
rm -rf temp_work
echo "All processes finished, temporary files cleaned up."
echo "###################################### All Finished ########################################"
