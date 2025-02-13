import pandas as pd
import os

# Define input and output directories
input_file = "eBay_Sold_Data.xlsx"
output_folder = "_Sold Data_"

# Create the folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Read all sheets
sheets = pd.read_excel(input_file, sheet_name=None)

# Save each sheet separately
for sheet_name, df in sheets.items():
    output_file = os.path.join(output_folder, f"{sheet_name}_Sold_Data_Unique.xlsx")
    df.to_excel(output_file, index=False)
    print(f"Saved: {output_file}")