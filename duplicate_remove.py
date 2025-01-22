import pandas as pd

# Load the Excel file
file_path = "Basketball_Sold_Data_Page_164.xlsx"  # Replace with your file path
data = pd.read_excel(file_path)

# Remove duplicate rows based on all columns, keeping the first occurrence
cleaned_data = data.drop_duplicates(keep='first')

# Save the cleaned data back to a new Excel file
output_path = "cleaned_file.xlsx"
cleaned_data.to_excel(output_path, index=False)

print(f"Duplicates removed! Cleaned file saved at: {output_path}")
