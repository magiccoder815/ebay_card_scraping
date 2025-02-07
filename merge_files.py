import pandas as pd
import os
from datetime import datetime, timedelta

# Define the path to your data folders
sold_data_folder = '_Sold Data_'
start_date = '2025-01-27'
end_date = '2025-02-06'
output_folder = 'Merged_Data'

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# List of sports to process
sports = ['Baseball', 'Basketball', 'Football']

# Generate the list of date strings
date_range = []
current_date = datetime.strptime(start_date, '%Y-%m-%d')
end_date = datetime.strptime(end_date, '%Y-%m-%d')

while current_date <= end_date:
    date_range.append(current_date.strftime('%Y-%m-%d'))
    current_date += timedelta(days=1)

for sport in sports:
    # Read the unique sold data
    unique_file = os.path.join(sold_data_folder, f'{sport}_Sold_Data_Unique.xlsx')
    unique_data = pd.read_excel(unique_file)

    # Initialize a list to hold daily data
    daily_data_list = []

    # Loop through date folders
    for date in date_range:
        daily_file = os.path.join(date, f'{sport}_{date}.xlsx')
        if os.path.exists(daily_file):
            daily_data = pd.read_excel(daily_file)
            daily_data_list.append(daily_data)

    # Concatenate all daily data
    if daily_data_list:
        all_daily_data = pd.concat(daily_data_list, ignore_index=True)
        # Merge with the unique data
        merged_data = pd.concat([unique_data, all_daily_data], ignore_index=True)

        # Sort the merged data by date (assuming there's a 'Date' column)
        if 'Date' in merged_data.columns:
            merged_data['Date'] = pd.to_datetime(merged_data['Date'])  # Convert to datetime
            merged_data = merged_data.sort_values(by='Date', ascending=False)  # Sort from latest to earliest

        # Save the merged data to a new Excel file
        output_file = os.path.join(output_folder, f'{sport}_Merged_Data.xlsx')
        merged_data.to_excel(output_file, index=False)

        print(f'Merged data for {sport} saved to {output_file}')
    else:
        print(f'No daily data found for {sport}.')

print('Merging process completed.')