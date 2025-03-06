import requests
from pymongo import MongoClient
import pandas as pd
from lxml import html

def product_list_fetch():
    try:
        url = "https://books.toscrape.com"
        pages = "/catalogue/page-1.html"
        all_data = []

        while pages:
            try:
                res = requests.get(url + pages)
                print(f"Fetching: {url + pages}, Status code: {res.status_code}")

                if res.status_code == 200:
                    tree = html.fromstring(res.content)

                    book_url = tree.xpath("//h3/a/@href")
                    book_title = tree.xpath('//h3/a/text()')
                    book_price = tree.xpath('//p[@class="price_color"]/text()')
                    book_in_stock = tree.xpath("//p[@class='instock availability']/text()[normalize-space()]")

                    # Remove spaces at the beginning and at the end of the string:
                    book_in_stock = [stock.strip() for stock in book_in_stock]

                    # Convert relative URLs to absolute URLs
                    book_url = [url + "/catalogue/" + link if "catalogue/" not in link else url + "/" + link for link in book_url]

                    all_data.extend(zip(book_url, book_title, book_price, book_in_stock))

                    next_page = tree.xpath('//ul[@class="pager"]/li[@class="next"]/a/@href')
                    pages = f"/catalogue/{next_page[0]}" if next_page else None

                else:
                    print(f"Failed to fetch page: {res.status_code}")
                    break
                
            except Exception as e:
                print(f"Error while fetching data: {e}")
                break

        if all_data:
            product_list_data = pd.DataFrame(all_data, columns=["book_url", "book_title", "book_price", "book_in_stock"])
            product_list_data["status"] = "Pending"  # Initially set to Pending
            data_store_database(product_list_data)  # Insert into database and update status
            data_in_excel(product_list_data)  # Save to Excel after updating status
        else:
            print("No data to store.")

    except Exception as e:
        print(f"Unexpected error: {e}")


def data_in_excel(product_list_data):
    try:
        file_path = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Books_to_scrape\\Books_List.xlsx"
        product_list_data.to_excel(file_path, index=False)
        print("Successfully saved in Excel.")
    except Exception as e:
        print(f"Error saving data to Excel: {e}")


def data_store_database(product_list_data):
    try:
        client = MongoClient("mongodb://admin:tP_kc8mn@localhost:27017/?authSource=admin")
        db = client['Book_to_scrape']
        collection = db['Books_list']

        print("Data insertion status: Pending")
        records = product_list_data.to_dict(orient='records')
        collection.insert_many(records)
        
        # Update status in DataFrame after successful insertion
        product_list_data["status"] = "Done"

        # Save the updated status in Excel
        data_in_excel(product_list_data)
        
        print("Data insertion status: Done")
    except Exception as e:
        print(f"Database error: {e}")


# Run the function
product_list_fetch()
