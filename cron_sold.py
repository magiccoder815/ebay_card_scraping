import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import re
import time
import threading
import pytz
import os
import gspread
from google.oauth2.service_account import Credentials

# Google Sheets setup
CREDENTIALS_FILE = "client_secret_1045032559705-gj7v7uuhv4i4vhee5h6n5rgig067bf83.apps.googleusercontent.com.json"  # Replace with the path to your credentials JSON file
SHEET_ID = "1nbwGW6GxqRpAYzzb0HEA7aMx7vCbb9RB"  # Replace with your Google Sheet ID
SHEET_NAME = "Sheet1"  # Replace with your sheet name

# Define the base URL template
base_url = "https://www.ebay.com/sch/i.html?_nkw=PSA+10&_sacat=0&_from=R40&LH_Sold=1&LH_Complete=1&_udlo=150&rt=nc&Sport={}&_dcat=261328&_ipg=240&_pgn={}"

# Define the EST timezone
est = pytz.timezone('America/New_York')

# Calculate the date for the previous day in EST
yesterday = datetime.now(est) - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
print("Yesterday's date in EST:", yesterday_str)

# Function to check if a sold date is from yesterday
def is_sold_yesterday(sold_date_str):
    try:
        sold_date = datetime.strptime(sold_date_str, "%b %d, %Y")
        return sold_date.strftime("%Y-%m-%d") == yesterday_str
    except ValueError:
        return False

# Start timer
start_time = time.time()

# List of sports to scrape
sports = ["Boxing"]

all_sold_data = []  # To store data for merging

def clean_set_name(set_name):
    return re.sub(r'^\d{4}(-\d{2})?\s*', '', set_name).strip()

# Define the proxy with authentication (if needed)
proxy = {
    "http": "http://username:password@proxy_address:port",
    "https": "http://username:password@proxy_address:port",
}

# Create a directory for today's date
date_folder = yesterday.strftime("%Y-%m-%d")
if not os.path.exists(date_folder):
    os.makedirs(date_folder)

try:
    for sport in sports:
        page = 1  # Reset page number for each sport
        encoded_sport = sport.replace(" ", "%2520")  # Manually replace spaces with %2520
        print(f"\nFetching data for {sport}...")

        sold_data = []  # Reset sold_data for the current sport

        while True:
            url = base_url.format(encoded_sport, page)
            print(url)
            response = requests.get(url)
            
            if response.status_code != 200:
                print(f"\nFailed to retrieve data from page {page} for {sport}: {response.status_code}")
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            sold_items = soup.find_all('li', class_='s-item s-item__pl-on-bottom')
            
            if not sold_items:
                print(f"\nNo more sold items found for {sport}.")
                break
                
            found_recent = False
            
            for item in sold_items:
                date_span = item.find('span', class_='s-item__caption--signal POSITIVE')
                sold_price_span = item.find('span', class_='s-item__price')

                if date_span:
                    sold_date_text = date_span.get_text(strip=True)
                    sold_date_text_cleaned = re.search(r'\b\w{3}\s\d{1,2},\s\d{4}\b', sold_date_text).group(0)
                    
                    if is_sold_yesterday(sold_date_text_cleaned):
                        sold_date = datetime.strptime(sold_date_text_cleaned, "%b %d, %Y").strftime("%Y-%m-%d")
                        link = item.find('a', class_='s-item__link')['href']
                        
                        sport_val = season_year = set_name = variation = player_name = ""
                        
                        if sold_price_span:
                            sold_price_text = sold_price_span.get_text(strip=True)
                            sold_price = sold_price_text
                        else:
                            sold_price = "N/A"

                        sold_data.append({
                            "Sport": sport_val or sport,
                            "Season Year": season_year,
                            "Set": set_name,
                            "Variation": variation,
                            "Player Name": player_name,
                            "Sold Price": sold_price,
                            "Sold Date": sold_date,
                            "Card Link": link
                        })
                        found_recent = True

            if not found_recent:
                print(f"\nNo recent sold items found on this page for {sport}.")
                break
            
            page += 1  # Go to the next page

        # Save the collected data to an Excel file for the current sport
        if sold_data:
            file_name = os.path.join(date_folder, f"{sport}_{yesterday.strftime('%Y-%m-%d')}.xlsx")
            df = pd.DataFrame(sold_data)
            df.to_excel(file_name, index=False)
            print(f"Sold data saved to '{file_name}'.")
            all_sold_data.extend(sold_data)  # Add to all sold data for merging

except KeyboardInterrupt:
    print("\nData collection interrupted. Saving collected data...")

# Merge and update Google Sheets
if all_sold_data:
    # Google Sheets authentication
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    gc = gspread.authorize(creds)

    # Open the Google Sheet
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.worksheet(SHEET_NAME)

    # Read existing data
    existing_data = pd.DataFrame(worksheet.get_all_records())

    # Merge the scraped data with existing data
    scraped_df = pd.DataFrame(all_sold_data)
    updated_data = pd.concat([existing_data, scraped_df]).drop_duplicates()

    # Update the Google Sheet
    worksheet.clear()
    worksheet.update([updated_data.columns.values.tolist()] + updated_data.values.tolist())

    print(f"Google Sheet updated successfully!")
