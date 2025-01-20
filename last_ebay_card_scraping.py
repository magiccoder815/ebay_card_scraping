import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import re
import time
import threading
import pytz
import os

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
sports = ["Football", "Baseball", "Ice Hockey", "Basketball"]
all_sold_data = []  # To store data for merging

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

# Define the proxy with authentication
username = '74176165-zone-custom-region-US-city-dallas-sessid-LKjJG6II'
password = 'Mlaunam3'
proxy = {
    "http": f"http://{username}:{password}@f.proxys5.net:6200",
    "https": f"http://{username}:{password}@f.proxys5.net:6200"
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
                if date_span:
                    sold_date_text = date_span.get_text(strip=True).replace("Sold ", "").strip()
                    
                    if is_sold_yesterday(sold_date_text):
                        sold_date = datetime.strptime(sold_date_text, "%b %d, %Y").strftime("%Y-%m-%d")
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
                        
                        sold_data.append({
                            "Sport": sport_val or sport,  # Store the sport being fetched
                            "Season Year": season_year,
                            "Set": set_name,
                            "Variation": variation,
                            "Player Name": player_name,
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

# Final save of merged data if there are any
if all_sold_data:
    merged_file_name = os.path.join(date_folder, f"{yesterday.strftime('%Y-%m-%d')}.xlsx")
    df_combined = pd.DataFrame(all_sold_data)
    df_combined.to_excel(merged_file_name, index=False)
    print(f"Merged sold data saved to '{merged_file_name}'.")

end_time = time.time()
execution_time = end_time - start_time
print(f"Total Execution Time: {execution_time:.2f} seconds")