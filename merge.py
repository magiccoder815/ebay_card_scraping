import os
import pandas as pd

# Define the root directory where your date-based folders are located
root_dir = "."  # Change this if needed
output_file = "Basketball_Sold_2025-02-19.xlsx"

def get_xlsx_files(root_dir):
    """Retrieve all Basketball_YYYY-MM-DD.xlsx files from date-based folders."""
    files = []
    for folder in os.listdir(root_dir):
        folder_path = os.path.join(root_dir, folder)
        if os.path.isdir(folder_path) and folder.startswith("2025-"):
            for file in os.listdir(folder_path):
                if file.startswith("Basketball_") and file.endswith(".xlsx"):
                    files.append(os.path.join(folder_path, file))
    return files

def merge_xlsx_files(output_file, files):
    """Merge all extracted files into the output file."""
    # Load the existing data
    if os.path.exists(output_file):
        df_master = pd.read_excel(output_file)
    else:
        df_master = pd.DataFrame()
    
    for file in files:
        df = pd.read_excel(file)
        df_master = pd.concat([df_master, df], ignore_index=True)
    
    # Remove duplicates if necessary
    df_master.drop_duplicates(inplace=True)
    
    # Save the merged file
    df_master.to_excel(output_file, index=False)
    print(f"Merged {len(files)} files into {output_file}")

if __name__ == "__main__":
    xlsx_files = get_xlsx_files(root_dir)
    if xlsx_files:
        merge_xlsx_files(output_file, xlsx_files)
    else:
        print("No matching files found.")