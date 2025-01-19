import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import re
import time
import threading

# Define the base URL template
base_url = "https://www.ebay.com/sch/i.html?_nkw=PSA+10&_sacat=0&_from=R40&LH_Sold=1&LH_Complete=1&_udlo=150&rt=nc&Sport=Baseball%7CFootball%7CIce%2520Hockey%7CBasketball&_dcat=261328&_ipg=240&_pgn={}&rt=nc"

# Calculate the date for the previous day
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# Function to check if a sold date is from yesterday
def is_sold_yesterday(sold_date_str):
    sold_date = datetime.strptime(sold_date_str, "%b %d, %Y")
    return sold_date.strftime("%Y-%m-%d") == yesterday_str

# Start timer
start_time = time.time()

# Start with the first page
page = 1
sold_data = []

def clean_set_name(set_name):
    return re.sub(r'^\d{4}\s+', '', set_name).strip()

def print_elapsed_time():
    while True:
        elapsed_time = time.time() - start_time
        print(f"Elapsed Time: {elapsed_time:.2f} seconds", end='\r')
        time.sleep(1)  # Update every second

# Start the elapsed time thread
elapsed_time_thread = threading.Thread(target=print_elapsed_time, daemon=True)
elapsed_time_thread.start()

try:
    while True:
        url = base_url.format(page)
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"\nFailed to retrieve data from page {page}: {response.status_code}")
            break
        
        soup = BeautifulSoup(response.text, 'html.parser')
        sold_items = soup.find_all('li', class_='s-item s-item__pl-on-bottom')
        
        if not sold_items:
            print("\nNo more sold items found.")
            break
            
        found_recent = False
        
        for item in sold_items:
            date_span = item.find('span', class_='s-item__caption--signal POSITIVE')
            if date_span:
                sold_date_text = date_span.get_text(strip=True)
                sold_date_text_cleaned = sold_date_text.replace("Sold ", "").strip()
                if is_sold_yesterday(sold_date_text_cleaned):
                    sold_date = datetime.strptime(sold_date_text_cleaned, "%b %d, %Y").strftime("%Y-%m-%d")
                    link = item.find('a', class_='s-item__link')['href']
                    
                    product_response = requests.get(link)
                    product_soup = BeautifulSoup(product_response.text, 'html.parser')
                    
                    sport = season_year = set_name = variation = player_name = ""
                    
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
                                    sport = value_text
                                elif name_text == "Season":
                                    season_year = value_text
                                elif name_text == "Set":
                                    set_name = clean_set_name(value_text)
                                elif name_text == "Parallel/Variety":
                                    variation = value_text
                                elif name_text == "Player/Athlete":
                                    player_name = value_text
                    
                    specifications_section_new = product_soup.find('div', {'data-testid': 'ux-layout-section-evo'})
                    if specifications_section_new:
                        details = specifications_section_new.find_all('dl')
                        for detail in details:
                            label = detail.find('dt').get_text(strip=True) if detail.find('dt') else ""
                            value = detail.find('dd').get_text(strip=True) if detail.find('dd') else ""
                            
                            if label == "Sport":
                                sport = value
                            elif label == "Season":
                                season_year = value
                            elif label == "Set":
                                set_name = clean_set_name(value)
                            elif label == "Parallel/Variety":
                                variation = value
                            elif label == "Player/Athlete":
                                player_name = value
                    
                    sold_data.append({
                        "Sport": sport,
                        "Season Year": season_year,
                        "Set": set_name,
                        "Variation": variation,
                        "Player Name": player_name,
                        "Sold Date": sold_date,
                        "Card Link": link
                    })
                    found_recent = True

        if not found_recent:
            print("\nNo recent sold items found on this page.")
            break
        
        page += 1

except KeyboardInterrupt:
    print("\nData collection interrupted. Saving collected data...")

# Save the collected data to an Excel file using yesterday's date in the filename
if sold_data:
    file_name = f"{yesterday.strftime('%Y-%m-%d')}.xlsx"
else:
    file_name = "No_Sold_Data.xlsx"

df = pd.DataFrame(sold_data)
df.to_excel(file_name, index=False)

# End timer and calculate duration
end_time = time.time()
execution_time = end_time - start_time
print(f"\nSold data saved to '{file_name}'.")
print(f"Total Execution Time: {execution_time:.2f} seconds")