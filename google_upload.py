import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os

# Define Google Sheets API setup
SERVICE_ACCOUNT_FILE = 'C:/ebay-sheet.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Define the list of files and their corresponding sport names
files = [
    ("Auto Racing_Sold_Data_Unique.xlsx", "Auto Racing"),
    ("Baseball_Sold_Data_Unique.xlsx", "Baseball"),
    ("Basketball_Sold_Data_Unique.xlsx", "Basketball"),
    ("Boxing_Sold_Data_Unique.xlsx", "Boxing"), 
    ("Breaking_Sold_Data_Unique.xlsx", "Breaking"),
    ("Football_Sold_Data_Unique.xlsx", "Football"),
    ("Ice Hockey_Sold_Data_Unique.xlsx", "Ice Hockey"),
    ("Mixed Martial Arts_Sold_Data_Unique.xlsx", "Mixed Martial Arts"),
    ("Soccer_Sold_Data_Unique.xlsx", "Soccer"),
    ("Wrestling_Sold_Data_Unique.xlsx", "Wrestling"),
]

try:
    # Authenticate and create a client
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)

    # Open the Google Sheet by its ID
    SPREADSHEET_ID = "1hwdmhFzl3WFxqJ7X3ugs9DqrGGMkonKnOT7wu_OdnyA"
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    print("Successfully accessed the Google Sheet!")

    for file_name, sport_name in files:
        # Access the first sheet or create a new one
        try:
            # Check if the worksheet already exists
            try:
                sheet = spreadsheet.worksheet(sport_name)
                # If it exists, delete it
                spreadsheet.del_worksheet(sheet)
                print(f"Deleted existing sheet: {sport_name}")
            except gspread.exceptions.WorksheetNotFound:
                print(f"No existing sheet found for {sport_name}. Creating a new one.")

            # Create a new worksheet with the sport name
            sheet = spreadsheet.add_worksheet(title=sport_name, rows="100", cols="20")
            
            # Load the Excel file into a DataFrame
            df = pd.read_excel(f'_Sold Data_/{file_name}')

            # Handle infinite and NaN values
            df.replace([float('inf'), float('-inf')], None, inplace=True)
            df.fillna('', inplace=True)

            # Set column headers
            column_headers = ["Sport", "Season Year", "Set", "Variation", "Player Name", "Sold Price", "Sold Date", "Card Number", "Card Link"]
            sheet.append_row(column_headers)  # Add column headers

            # Prepare data for appending
            data = df.values.tolist()  # Convert DataFrame to a list of lists

            # Append data to the new sheet
            sheet.append_rows(data)  # Append the data
            print(f"Data from {file_name} successfully appended to {sport_name} tab.")

        except gspread.exceptions.APIError as api_error:
            print(f"API Error for {sport_name}: {api_error}")
        except Exception as e:
            print(f"An error occurred for {sport_name}: {e}")

except gspread.exceptions.APIError as api_error:
    print(f"API Error: {api_error}")
except PermissionError as perm_error:
    print(f"Permission Error: {perm_error}")
except FileNotFoundError:
    print("The service account file was not found.")
except Exception as e:
    print(f"An error occurred: {e}")