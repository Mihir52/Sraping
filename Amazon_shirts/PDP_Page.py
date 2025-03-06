import requests, pymysql, os, json
from parsel import Selector
import hashlib
import unicodedata
import html, re
from lxml import html
from threading import Thread
import pandas as pd
import mysql.connector
from parsel import Selector

# database create 
def database():
    try:
        db = pymysql.connect(host="localhost",user="root",password="xbyte")
        cursor = db.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS Amazon_shirts")
        db.commit()
    except Exception as e:
        print("Database creation error**: ",e)
    
    try:
        db = pymysql.connect(host="localhost",user="root",password="xbyte",database="Amazon_shirts")
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PDP_DATA(
                Product_url TEXT,
                Product_id VARCHAR(50) NOT NULL,
                Product_price VARCHAR(50) NOT NULL,
                Product_color TEXT,
                Status VARCHAR(50) DEFAULT "Pending")
        """)
    except Exception as e:
        print("Database table error** :",e)

    return db,cursor

# Fetch product urls through database
def fetch_product_urls():
    conn, cursor = database()
    try:
        query = "SELECT Product_url,Product_price,Product_id FROM shirts"  
        cursor.execute(query)
        urls = cursor.fetchall()
        print(urls)
        return urls

    except Exception as e:
        print("Error fetching product URLs:", e)

    finally:
        cursor.close()
        conn.close()

# fetch inner product details through product url
def scrape_product_details(url, product_price, product_id):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            tree = html.fromstring(response.content)
            
            s = Selector(text=response.text)

            product_colors = []
            color_ids = s.xpath("//li[contains(@id,'color_name')]/@data-defaultasin").getall()

            for color_id in color_ids:
                color_page_url = f"https://www.amazon.com/dp/{color_id}"  
                color_response = requests.get(color_page_url)

                if color_response.status_code == 200:
                    color_selector = Selector(text=color_response.text)
                    # color_name = color_selector.xpath('//div[@class="a-row"]//span[@class="selection"]/text()').get()
                    color_name = color_selector.xpath('//div[@id="variation_color_name"]/div[@class="a-row"]/span[@class="selection"]/text()').get()
                    # color_name = color_selector.xpath('//*[@class="imgSwatch"]//@alt').get()

                    if color_name:
                        # cleaned_color = clean_color_name(color_name)
                        # product_colors.append(color_name.strip())
                        product_colors.append(color_name)
                        
            print(product_colors)
            product_str = ','.join(product_colors)
            # product_colors = product_str
            # print(product_colors)
            # print(len(product_colors))
            
            print(f"Fetching : {url}")

            # scraped_rows = [(url, product_id, product_price, color) for color in product_colors]
            scraped_rows = []
            for color in product_colors:
                scraped_rows.append((url,product_id,product_price,color))
            return scraped_rows
            # return (url, product_id, product_price, product_colors)
            

    except Exception as e:
        print("Error scraping:", e)
        return None

# Insert in database
def insert_product_details(data):
    conn, cursor = database()

    try:
        query = """
            INSERT INTO PDP_DATA (Product_url, Product_id, Product_price, Product_color, Status)
            VALUES (%s, %s, %s, %s, 'Done')
        """
        cursor.executemany(query, data)
        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print("Database Insertion Error:", e)

    finally:
        cursor.close()
        conn.close()
    
# Main function to combine all functions and excute code 
def main():
    product_urls = fetch_product_urls()

    if not product_urls:
        print("No product URLs found.")
        return  # stops excecution

    scraped_data = []
    for url,product_price,product_id in product_urls:
        details = scrape_product_details(url,product_price,product_id)
        if details:
            scraped_data.extend(details)

    if scraped_data:
        insert_product_details(scraped_data)
        data_store = pd.DataFrame(scraped_data,columns=['Product_url', 'Product_id', 'Product_price', 'Product_color'])
        data_in_excel(data_store)
    print("Process Completed!")


def data_in_excel(data_store):
    try:
        data_store.to_excel("C:\\Users\\mihir.parate\\Desktop\\Python task\\Amazon_shirts\\Shirts_pdp.xlsx",index=False)
        print("Successfully insert in Excel.")
        
    except Exception as e:
        print("****Excel data store*****",e)

# def clean_color_name(color_name):
#     cleaned_name = re.sub(r"\s-\s.*", "", color_name)
#     cleaned_name = re.sub(r"\s*\(.*?\)", "", color_name)
    
    # return cleaned_name.strip()

main()
