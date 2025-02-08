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
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path

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
sports = [
    "Auto Racing", 
    "Boxing", 
    "Baseball", 
    "Breaking", 
    "Football", 
    "Ice Hockey", 
    "Soccer", 
    "Wrestling", 
    "Mixed Martial Arts"
]

def clean_set_name(set_name):
    return re.sub(r'^\d{4}(-\d{2})?\s*', '', set_name).strip()

def print_elapsed_time():
    while True:
        elapsed_time = time.time() - start_time
        print(f"Elapsed Time: {elapsed_time:.2f} seconds", end='\r')
        time.sleep(1)

# Start the elapsed time thread
elapsed_time_thread = threading.Thread(target=print_elapsed_time, daemon=True)
elapsed_time_thread.start()

# Create a directory for yesterday's date
date_folder = yesterday.strftime("%Y-%m-%d")
os.makedirs(date_folder, exist_ok=True)

# Google Sheets setup
SERVICE_ACCOUNT_FILE = Path(__file__).parent / "../keys/ebay-sheet.json"
SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE.resolve()  # Convert to absolute path
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(str(SERVICE_ACCOUNT_FILE), scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1hwdmhFzl3WFxqJ7X3ugs9DqrGGMkonKnOT7wu_OdnyA/edit?gid=0')

try:
    for sport in sports:
        page = 1
        if sport == "Mixed Martial Arts":
            encoded_sport = "Mixed%2520Martial%2520Arts%2520%2528MMA%2529"
        else:
            encoded_sport = sport.replace(" ", "%2520")
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
            
            # Check if the number of sold items is less than 239
            if len(sold_items) < 239:
                print(f"\nLast page reached for {sport}, found {len(sold_items)} sold items.")
                # Scrape data from this last page
                if sold_items:
                    for item in sold_items:
                        date_span = item.find('span', class_='s-item__caption--signal POSITIVE')
                        sold_price_span = item.find('span', class_='s-item__price')

                        if date_span:
                            sold_date_text = date_span.get_text(strip=True)
                            sold_date_text_cleaned = re.search(r'\b\w{3}\s\d{1,2},\s\d{4}\b', sold_date_text).group(0)
                            
                            if is_sold_yesterday(sold_date_text_cleaned):
                                sold_date = datetime.strptime(sold_date_text_cleaned, "%b %d, %Y").strftime("%Y-%m-%d")
                                link = item.find('a', class_='s-item__link')['href']
                                
                                # Fetch product details
                                product_response = requests.get(link)
                                product_soup = BeautifulSoup(product_response.text, 'html.parser')
                                
                                sport_val = season_year = set_name = variation = player_name = ""
                                
                                # Attempt to find specifications
                                specifications_section = product_soup.find('section', class_='product-spectification')
                                if specifications_section:
                                    details = specifications_section.find_all('li')
                                    for detail in details:
                                        name = detail.find('div', class_='s-name')
                                        value = detail.find('div', class_='s-value')
                                        if name and value:
                                            name_text = name.get_text(strip=True)
                                            value_text = value.get_text(strip=True)
                                            if name_text == "Sport":
                                                sport_val = value_text
                                            elif name_text == "Season":
                                                season_year = value_text
                                            elif name_text == "Set":
                                                set_name = clean_set_name(value_text)
                                            elif name_text == "Parallel/Variety":
                                                variation = value_text
                                            elif name_text == "Player/Athlete":
                                                player_name = value_text
                                
                                # New specifications section
                                specifications_section_new = product_soup.find('div', {'data-testid': 'ux-layout-section-evo'})
                                if specifications_section_new:
                                    details = specifications_section_new.find_all('dl')
                                    for detail in details:
                                        label = detail.find('dt').get_text(strip=True) if detail.find('dt') else ""
                                        value = detail.find('dd').get_text(strip=True) if detail.find('dd') else ""
                                        
                                        if label == "Sport":
                                            sport_val = value
                                        elif label == "Season":
                                            season_year = value
                                        elif label == "Set":
                                            set_name = clean_set_name(value)
                                        elif label == "Parallel/Variety":
                                            variation = value
                                        elif label == "Player/Athlete":
                                            player_name = value
                                
                                if sold_price_span:
                                    sold_price_text = sold_price_span.get_text(strip=True)
                                    sold_price = sold_price_text
                                else:
                                    sold_price = "N/A"

                                sold_data.append({
                                    "Sport": sport_val,
                                    "Season Year": season_year,
                                    "Set": set_name,
                                    "Variation": variation,
                                    "Player Name": player_name,
                                    "Sold Price": sold_price,
                                    "Sold Date": sold_date,
                                    "Card Link": link
                                })

                break  # Exit loop after scraping the last page

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
                        
                        # Fetch product details
                        product_response = requests.get(link)
                        product_soup = BeautifulSoup(product_response.text, 'html.parser')
                        
                        sport_val = season_year = set_name = variation = player_name = ""
                        
                        # Attempt to find specifications
                        specifications_section = product_soup.find('section', class_='product-spectification')
                        if specifications_section:
                            details = specifications_section.find_all('li')
                            for detail in details:
                                name = detail.find('div', class_='s-name')
                                value = detail.find('div', class_='s-value')
                                if name and value:
                                    name_text = name.get_text(strip=True)
                                    value_text = value.get_text(strip=True)
                                    if name_text == "Sport":
                                        sport_val = value_text
                                    elif name_text in ["Season", "Year"]:
                                        season_year = value_text
                                    elif name_text == "Set":
                                        set_name = clean_set_name(value_text)
                                    elif name_text == "Parallel/Variety":
                                        variation = value_text
                                    elif name_text in ["Player/Athlete", "Player"]:
                                        player_name = value_text
                        
                        # New specifications section
                        specifications_section_new = product_soup.find('div', {'data-testid': 'ux-layout-section-evo'})
                        if specifications_section_new:
                            details = specifications_section_new.find_all('dl')
                            for detail in details:
                                label = detail.find('dt').get_text(strip=True) if detail.find('dt') else ""
                                value = detail.find('dd').get_text(strip=True) if detail.find('dd') else ""
                                
                                if label == "Sport":
                                    sport_val = value
                                elif label in ["Season", "Year"]:
                                    season_year = value
                                elif label == "Set":
                                    set_name = clean_set_name(value)
                                elif label == "Parallel/Variety":
                                    variation = value
                                elif label in ["Player/Athlete", "Player"]:
                                    player_name = value
                        
                        if sold_price_span:
                            sold_price_text = sold_price_span.get_text(strip=True)
                            sold_price = sold_price_text
                        else:
                            sold_price = "N/A"

                        sold_data.append({
                            "Sport": sport_val,
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
            print("sport: ", sport)

            # Check if the worksheet exists, if not create it
            worksheet = spreadsheet.worksheet(sport)
            # Append the new data to the corresponding worksheet
            # Define expected headers
            expected_headers = ["Sport", "Season Year", "Set", "Variation", "Player Name", "Sold Price", "Sold Date", "Card Link"]

            # Read existing data from Google Sheet with expected headers
            existing_data = pd.DataFrame(worksheet.get_all_records(expected_headers=expected_headers))

            # Combine the new sold data with existing data
            new_data_df = pd.DataFrame(sold_data)
            combined_df = pd.concat([new_data_df, existing_data], ignore_index=True)

            # Convert 'Sold Date' to datetime and sort by it
            combined_df['Sold Date'] = pd.to_datetime(combined_df['Sold Date'])
            combined_df = combined_df.sort_values(by='Sold Date', ascending=False)

            # Convert 'Sold Date' back to string format for JSON serialization
            combined_df['Sold Date'] = combined_df['Sold Date'].dt.strftime('%Y-%m-%d')

            # Clear the existing data in the Google Sheet
            worksheet.clear()  # Clear all data except headers
            worksheet.append_row(combined_df.columns.tolist())  # Append header
            worksheet.append_rows(combined_df.values.tolist())   # Append data

            print(f"Merged sold data saved to the Google Sheet, sorted by sold date.")

except KeyboardInterrupt:
    print("\nData collection interrupted. Saving collected data...")

end_time = time.time()
execution_time = end_time - start_time
print(f"Total Execution Time: {execution_time:.2f} seconds")