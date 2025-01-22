import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re
import time
import threading
import os

# Define the base URL template
base_url = "https://www.ebay.com/sch/i.html?_nkw=PSA+10&_sacat=0&_from=R40&LH_Sold=1&LH_Complete=1&Sport={}&_dcat=261328&_udlo=150&_ipg=240&_pgn={}&rt=nc"
sport_name = "Baseball"
encoded_sport = sport_name.replace(" ", "%2520")

# Start timer
start_time = time.time()

# Start with page 14
page = 1
sold_data = []

def clean_set_name(set_name):
    return re.sub(r'^\d{4}(-\d{2})?\s*', '', set_name).strip()

def print_elapsed_time():
    while True:
        elapsed_time = time.time() - start_time
        print(f"Total Elapsed Time: {elapsed_time:.2f} seconds", end='\r')
        time.sleep(1)  # Update every second

# Start the elapsed time thread
elapsed_time_thread = threading.Thread(target=print_elapsed_time, daemon=True)
elapsed_time_thread.start()

# Create directory for sport data
if not os.path.exists(sport_name):
    os.makedirs(sport_name)

# Define the proxy with authentication
username = '74176165-zone-custom-region-US-city-miami-sessid-0AZY4sVH'
password = 'Mlaunam3'
proxy = {
    "http": f"http://{username}:{password}@f.proxys5.net:6200",
    "https": f"http://{username}:{password}@f.proxys5.net:6200"
}

try:
    while True:  # Continue indefinitely until no sold items are found
        print("Page number:", page)
        page_start_time = time.time()  # Start time for the current page
        url = base_url.format(encoded_sport, page)
        print(url)
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"\nFailed to retrieve data from page {page}: {response.status_code}")
            break
        
        soup = BeautifulSoup(response.text, 'html.parser')
        sold_items = soup.find_all('li', class_='s-item s-item__pl-on-bottom')
        
        # Always scrape the current page
        sold_items_count = len(sold_items)

        print(f"\nSold items: {sold_items_count}")
        
        if sold_items_count < 239:
            print(f"\nFewer than 239 sold items found on this page: {sold_items_count}.")
        
        if not sold_items:
            print("\nNo more sold items found.")
            break
            
        found_recent = False
        
        for item in sold_items:
            date_span = item.find('span', class_='s-item__caption--signal POSITIVE')
            sold_price_span = item.find('span', class_='s-item__price')
            if date_span:
                sold_date_text = date_span.get_text(strip=True)
                sold_date_text_cleaned = re.search(r'\b\w{3}\s\d{1,2},\s\d{4}\b', sold_date_text)
                if sold_date_text_cleaned:
                    sold_date = datetime.strptime(sold_date_text_cleaned.group(0), "%b %d, %Y").strftime("%Y-%m-%d")
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
                    if sold_price_span:
                        sold_price_text = sold_price_span.get_text(strip=True)  # Get the price text
                        sold_price = sold_price_text  # You might want to further process this if necessary
                    else:
                        sold_price = "N/A"  # Default value if price not found

                    sold_data.append({
                        "Sport": sport,
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
            print("\nNo recent sold items found on this page.")
            break

        # Save the collected data for the current page to an Excel file
        df = pd.DataFrame(sold_data)
        page_filename = os.path.join(sport_name, f"{sport_name}_Sold_Data_Page_{page}.xlsx")
        df.to_excel(page_filename, index=False)
        print(f"\nData for page {page} saved to '{page_filename}'.")

        page_end_time = time.time()  # End time for the current page
        page_duration = page_end_time - page_start_time
        print(f"\nTime taken for page {page}: {page_duration:.2f} seconds")  # Print time for the current page
        
        # Stop scraping after the current page if fewer than 239 items
        if sold_items_count < 239:
            print("\nStopping scraping after this page.")
            break
        
        page += 1  # Increment page number for the next iteration

except KeyboardInterrupt:
    print("\nData collection interrupted. Saving collected data...")

# Save the final collected data to an Excel file
df_final = pd.DataFrame(sold_data)
final_filename = os.path.join(sport_name, f"{sport_name}_Sold_Data_Final.xlsx")
df_final.to_excel(final_filename, index=False)

# End timer and calculate duration
end_time = time.time()
execution_time = end_time - start_time
print(f"\nSold data saved to '{final_filename}'.")
print(f"Total Execution Time: {execution_time:.2f} seconds")