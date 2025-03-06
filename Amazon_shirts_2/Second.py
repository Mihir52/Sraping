import requests, pymysql, os, json
import hashlib
import unicodedata
import html, re
from googlesearch import url_search
from lxml import html
import pandas as pd
from parsel import Selector
from threading import Thread
import mysql.connector

# database create
def database():
    try:
        db = pymysql.connect(host="localhost", user="root", password="xbyte")
        cursor = db.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS amazon_shirts_2")
        db.commit()
    except Exception as e:
        print("Database creation error**: ", e)

    try:
        db = pymysql.connect(host="localhost", user="root", password="xbyte", database="amazon_shirts_2")
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PDP_PAGE(
                Product_id VARCHAR(50) NOT NULL,
                Product_url TEXT,
                Product_name VARCHAR(100),
                Product_color TEXT,
                Product_Size TEXT,
                Product_price VARCHAR(50),
                Status VARCHAR(50) DEFAULT "Pending")
            """)
    except Exception as e:
        print("Database table error** :", e)

    return db,cursor

# Fetch product urls through database
def fetch_product_urls():
    conn, cursor = database()
    try:
        query = "SELECT Product_url,Product_id FROM PL_Page"
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
def scrape_product_details(url,product_id):
    try:
        response = requests.get(url)
        print(url)
        print("Status code : ",response.status_code)

        if response.status_code == 200:
            # tree = html.fromstring(response.content)
            s = Selector(text=response.text)

            product_name = s.xpath('//span[@id="productTitle"]/text()').get()
            print(product_name)

            product_colors = s.xpath("//li[contains(@id,'color_name')]/@data-defaultasin").getall()

            # product_size = s.xpath("//li[contains(@class,'dropdownAvailable')]/a/text()").getall()
            product_size = s.xpath("//ul[contains(@class, 'a-list-link')]/li/a/text()").getall()
            product_price = s.xpath('//span[@class="a-price-range"]/span[1]/span[@class="a-offscreen"]/text()').get()

            return (((url,product_id,product_name,product_colors,product_size,product_price)))
    except Exception as e:
        print("Error scraping:", e)
        return None

# Main function
def main():
    product_urls = fetch_product_urls()
    if not product_urls:
        print("No product URLs found.")
        return
    scraped_data = []
    for url,product_id in product_urls:
        details = scrape_product_details(url,product_id)
        if details:
            scraped_data.append(details)

    if scraped_data:
        insert_product_details(scraped_data)
        data_in_excel()
        print("Process Completed!")

# Insert into db
def insert_product_details(data):
    conn, cursor = database()
    try:
        query = """
            INSERT INTO PDP_PAGE (Product_id,Product_url,Product_name,Product_color,Product_Size,Product_price,Status)
            VALUES (%s, %s, %s, %s, %s, %s,'Done')
        """
        cursor.executemany(query, data)
        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print("Database Insertion Error:", e)

    finally:
        cursor.close()
        conn.close()

def data_in_excel():
    conn, cursor = database()
    try:
        table_name = "PDP_PAGE"
        file_name = f"{table_name}.xlsx"
        directory = f"C:\\Users\\mihir.parate\\Desktop\\Python task\\Amazon_shirts_2\\{file_name}"
        os.makedirs(directory, exist_ok=True)

        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            # The first element (desc[0]) of each tuple is the column name.
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            df.to_excel(file_name, index=False)
            print("Excel file created successfully!")
            print("Insert data in excel file!!")
        else:
            print("No data available to write to Excel.")

    except Exception as e:
        print("Excel file error:", e)
    finally:
        cursor.close()
        conn.close()

main()
