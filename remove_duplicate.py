import pandas as pd
import os

# List of folders

# Iterate through each folder
# Construct the file path for the Excel file in the current folder
file_path = os.path.join("Basketball_Sold_Data_Page_154.xlsx")  # Adjust the file name as needed

# Check if the file exists
if os.path.exists(file_path):
    # Load the Excel file
    df = pd.read_excel(file_path)

    # Remove duplicates based on all columns except 'Card Link', keeping the first occurrence
    df_unique = df.drop_duplicates(subset=df.columns.difference(['Card Link']))

    # Save the updated DataFrame to a new Excel file in the same folder
    output_file_path = os.path.join("Basketball_Sold_Data_Unique.xlsx")
    df_unique.to_excel(output_file_path, index=False)

    print(f"Updated file saved as: {output_file_path}")
else:
    print(f"File not found: {file_path}")