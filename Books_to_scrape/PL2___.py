import requests
from pymongo import MongoClient
import pandas as pd
from lxml import html
from urllib.parse import urljoin
import os

# File Paths
DATA_FILE = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Books_to_scrape\\Books_List.xlsx"
HTML_SAVE_PATH = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Books_to_scrape\\html_pages\\"

# Ensure the HTML save directory exists
os.makedirs(HTML_SAVE_PATH, exist_ok=True)

def product_list_fetch():
    try:
        url = "https://books.toscrape.com"
        pages = "/catalogue/page-1.html"
        all_data = []

        # Load existing data to avoid refetching "Done" records
        existing_data = load_existing_data()
        fetched_urls = set(existing_data["book_url"]) if existing_data is not None else set()

        while pages:
            try:
                full_url = url + pages
                res = requests.get(full_url)
                print(f"Fetching: {full_url}, Status code: {res.status_code}")

                if res.status_code == 200:
                    # Save the HTML page
                    save_html_page(full_url, res.text)

                    tree = html.fromstring(res.content)

                    # Extract product details
                    book_urls = tree.xpath("//h3/a/@href")
                    book_titles = tree.xpath('//h3/a/text()')
                    book_prices = tree.xpath('//p[@class="price_color"]/text()')
                    book_in_stock = tree.xpath("//p[@class='instock availability']/text()[normalize-space()]")

                    # Convert relative URLs to absolute
                    book_urls = [urljoin(url + "/catalogue/", link) for link in book_urls]

                    # Ensure stock data is clean
                    book_in_stock = [stock.strip() for stock in book_in_stock]

                    # Add only new books that haven't been fetched yet
                    for i in range(len(book_titles)):
                        if book_urls[i] not in fetched_urls:
                            all_data.append({
                                "book_url": book_urls[i],
                                "book_title": book_titles[i],
                                "book_price": book_prices[i],
                                "book_in_stock": book_in_stock[i] if i < len(book_in_stock) else "Unknown",
                                "status": "Pending"
                            })

                    # Find next page URL
                    next_page = tree.xpath('//ul[@class="pager"]/li[@class="next"]/a/@href')
                    pages = f"/catalogue/{next_page[0]}" if next_page else None

                else:
                    print(f"Failed to fetch page: {res.status_code}")
                    break

            except Exception as e:
                print(f"Error while fetching data: {e}")
                break

        if all_data:
            product_list_data = pd.DataFrame(all_data)
            data_in_excel(product_list_data, append=True)  # Append new data
            data_store_database(all_data)  # Insert only new records
        else:
            print("No new data to store.")

    except Exception as e:
        print(f"Unexpected error: {e}")

def save_html_page(url, content):
    """ Saves the fetched HTML content locally for reference. """
    filename = os.path.join(HTML_SAVE_PATH, url.replace("https://", "").replace("/", "_") + ".html")
    with open(filename, "w",    ) as f:
        f.write(content)
    print(f"Saved HTML: {filename}")

def load_existing_data():
    """ Loads existing data from Excel to avoid refetching 'Done' records. """
    if os.path.exists(DATA_FILE):
        return pd.read_excel(DATA_FILE)
    return None

def data_in_excel(product_list_data, append=False):
    """ Saves data to Excel. If append=True, merges with existing data. """
    try:
        if append and os.path.exists(DATA_FILE):
            existing_data = pd.read_excel(DATA_FILE)
            product_list_data = pd.concat([existing_data, product_list_data], ignore_index=True)

        product_list_data.to_excel(DATA_FILE, index=False)
        print("Successfully saved in Excel.")
    except Exception as e:
        print(f"Error saving data to Excel: {e}")

def data_store_database(records):
    """ Inserts data into MongoDB and updates status to 'Done'. """
    try:
        client = MongoClient("mongodb://admin:tP_kc8mn@localhost:27017/?authSource=admin")
        db = client['Book_to_scrape']
        collection = db['Books_list']

        print("Data insertion status: Pending")

        # Insert records into MongoDB
        collection.insert_many(records)

        # Update all inserted records to "Done"
        for record in records:
            collection.update_one({"book_url": record["book_url"]}, {"$set": {"status": "Done"}})

        # Update Excel after changing status to "Done"
        update_excel_status()

        print("Data insertion status: Done")
    except Exception as e:
        print(f"Database error: {e}")

def update_excel_status():
    """ Updates Excel file to reflect 'Done' status for already inserted records. """
    try:
        product_list_data = pd.read_excel(DATA_FILE)
        client = MongoClient("mongodb://admin:tP_kc8mn@localhost:27017/?authSource=admin")
        db = client['Book_to_scrape']
        collection = db['Books_list']

        # Get book URLs from MongoDB where status is 'Done'
        done_books = set(record["book_url"] for record in collection.find({"status": "Done"}, {"book_url": 1}))

        # Update status in DataFrame
        product_list_data.loc[product_list_data["book_url"].isin(done_books), "status"] = "Done"

        # Save updated Excel file
        product_list_data.to_excel(DATA_FILE, index=False)
        print("Excel status updated.")
    except Exception as e:
        print(f"Error updating Excel status: {e}")

# Run the function
product_list_fetch()
