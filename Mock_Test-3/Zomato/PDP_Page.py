from cffi.cffi_opcode import PRIM_INT
from encodings.hex_codec import hex_decode

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

cookies = {
    'PHPSESSID': 'efb1042c1d9f827ec410220177f3f51a',
    'csrf': '3a814420ec37beda1ff0bc593f8e8aa3',
    'fbcity': '11722',
    'fre': '0',
    'rd': '1380000',
    'zl': 'en',
    'fbtrack': 'e370c80a2a04f6b58ce5f7e780211777',
    'ltv': '173508',
    'lty': '173508',
    'locus': '%7B%22addressId%22%3A0%2C%22lat%22%3A10.77052%2C%22lng%22%3A76.39898%2C%22cityId%22%3A11722%2C%22ltv%22%3A173508%2C%22lty%22%3A%22subzone%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A79852%7D',
    '_gid': 'GA1.2.997639609.1741340340',
    '_gcl_au': '1.1.214743333.1741340341',
    '_fbp': 'fb.1.1741340343378.842193404776764524',
    '_ga_DYN7ZGYTHM': 'GS1.2.1741342240.1.0.1741342240.0.0.0',
    '_ga_L7TYXFHY80': 'GS1.2.1741342240.1.0.1741342240.0.0.0',
    'ak_bmsc': 'FDBD740FCCB95D5F2E90DA5D66185E13~000000000000000000000000000000~YAAQLXLBF17bWUiVAQAA70CccBtUDJELozIx5KiyJMIamr26YYSa2OFw3vhsQ4XVL/EIU239h+o1yCwtjFrrwU6bOVHFsGealYt/hYAYfL/FDSF6fQSxhYMc+yAZD9CPNlgf6uNR3UR1li5g4Yde4XPIWSFDZNudWL1se4bn6mIr0pV46J+PLw7moKok1lup2s3jYOgRz8j1oA1in24nHK4LaFh+HlU7OrG2qHcz2U946+4zBxm1RdoHgXGpINEte75yqXE06WLHvz3CUJxO6OFlZRr0mW7VzWMk5DsbZLbM4r60GLhLuO1HemiT/5DRCciDR2nuPUvPZkf+UpagGX2KTlQ0CbwTUib1u1oFXHmfutYQLlVuno0JML0te4NRFoIvbbq8aZeE',
    '_ga': 'GA1.1.133479830.1741340340',
    '_gat_global': '1',
    '_gat_country': '1',
    '_ga_X6B66E85ZJ': 'GS1.2.1741352995.4.1.1741357577.55.0.0',
    '_dd_s': 'rum=0&expire=1741358484209',
    'AWSALBTG': 'ltkzbghoWoWexj9OAB6uuzmDt6v00QBySRMxdwPI5KbiOwPS3yz3DBy9Iu2i1n5bA4nX8c7oEot8HvaYXy/MlQOG52+AFDEgv89rw78GesBJUilloKIvGVplAtctWH3EwEQ2PIeustoynhO08BnvjHiHdiwb3NRZS+9QW2lu5jIC',
    'AWSALBTGCORS': 'ltkzbghoWoWexj9OAB6uuzmDt6v00QBySRMxdwPI5KbiOwPS3yz3DBy9Iu2i1n5bA4nX8c7oEot8HvaYXy/MlQOG52+AFDEgv89rw78GesBJUilloKIvGVplAtctWH3EwEQ2PIeustoynhO08BnvjHiHdiwb3NRZS+9QW2lu5jIC',
    '_ga_2XVFHLPTVP': 'GS1.1.1741352930.4.1.1741357590.42.0.0',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    # 'cookie': 'PHPSESSID=efb1042c1d9f827ec410220177f3f51a; csrf=3a814420ec37beda1ff0bc593f8e8aa3; fbcity=11722; fre=0; rd=1380000; zl=en; fbtrack=e370c80a2a04f6b58ce5f7e780211777; ltv=173508; lty=173508; locus=%7B%22addressId%22%3A0%2C%22lat%22%3A10.77052%2C%22lng%22%3A76.39898%2C%22cityId%22%3A11722%2C%22ltv%22%3A173508%2C%22lty%22%3A%22subzone%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A79852%7D; _gid=GA1.2.997639609.1741340340; _gcl_au=1.1.214743333.1741340341; _fbp=fb.1.1741340343378.842193404776764524; _ga_DYN7ZGYTHM=GS1.2.1741342240.1.0.1741342240.0.0.0; _ga_L7TYXFHY80=GS1.2.1741342240.1.0.1741342240.0.0.0; ak_bmsc=FDBD740FCCB95D5F2E90DA5D66185E13~000000000000000000000000000000~YAAQLXLBF17bWUiVAQAA70CccBtUDJELozIx5KiyJMIamr26YYSa2OFw3vhsQ4XVL/EIU239h+o1yCwtjFrrwU6bOVHFsGealYt/hYAYfL/FDSF6fQSxhYMc+yAZD9CPNlgf6uNR3UR1li5g4Yde4XPIWSFDZNudWL1se4bn6mIr0pV46J+PLw7moKok1lup2s3jYOgRz8j1oA1in24nHK4LaFh+HlU7OrG2qHcz2U946+4zBxm1RdoHgXGpINEte75yqXE06WLHvz3CUJxO6OFlZRr0mW7VzWMk5DsbZLbM4r60GLhLuO1HemiT/5DRCciDR2nuPUvPZkf+UpagGX2KTlQ0CbwTUib1u1oFXHmfutYQLlVuno0JML0te4NRFoIvbbq8aZeE; _ga=GA1.1.133479830.1741340340; _gat_global=1; _gat_country=1; _ga_X6B66E85ZJ=GS1.2.1741352995.4.1.1741357577.55.0.0; _dd_s=rum=0&expire=1741358484209; AWSALBTG=ltkzbghoWoWexj9OAB6uuzmDt6v00QBySRMxdwPI5KbiOwPS3yz3DBy9Iu2i1n5bA4nX8c7oEot8HvaYXy/MlQOG52+AFDEgv89rw78GesBJUilloKIvGVplAtctWH3EwEQ2PIeustoynhO08BnvjHiHdiwb3NRZS+9QW2lu5jIC; AWSALBTGCORS=ltkzbghoWoWexj9OAB6uuzmDt6v00QBySRMxdwPI5KbiOwPS3yz3DBy9Iu2i1n5bA4nX8c7oEot8HvaYXy/MlQOG52+AFDEgv89rw78GesBJUilloKIvGVplAtctWH3EwEQ2PIeustoynhO08BnvjHiHdiwb3NRZS+9QW2lu5jIC; _ga_2XVFHLPTVP=GS1.1.1741352930.4.1.1741357590.42.0.0',
}

# response = requests.get(
#     'https://www.zomato.com/ottapalam/hotel-nalandha-ottapalam-locality/order',
#     cookies=cookies,
#     headers=headers,
# )

html_pages = 1

# database create
def database():
    try:
        db = pymysql.connect(host="localhost", user="root", password="xbyte")
        cursor = db.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS Mock_test_3")
        db.commit()
    except Exception as e:
        print("Database creation error**: ", e)

    try:
        db = pymysql.connect(host="localhost", user="root", password="xbyte", database="Mock_test_3")
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Zomato_pdp_page(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_name TEXT,
                Product_price VARCHAR(50),
                Product_desc TEXT,
                Img_url TEXT,
                Product_url TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID))
            """)
    except Exception as e:
        print("Database table error** :", e)

    return db, cursor


# Fetch product urls through database
def fetch_product_urls():
    conn, cursor = database()
    try:
        query = "SELECT Product_url FROM Zomato_Pl"
        cursor.execute(query)
        urls = cursor.fetchall()
        for row in product_urls:
            url = row[0]
        # urls = [url[0] for url in urls]
        # print(urls)
        return urls

    except Exception as e:
        print("Error fetching product URLs:", e)

    finally:
        cursor.close()
        conn.close()

# fetch inner product details through product url
def scrape_product_details(url):
    try:
        response = requests.get(url,headers=headers,cookies=cookies)
        # print(f"Product ID: {product_id}, Length: {len(product_id)}")

        if response.status_code == 200:
            tree = html.fromstring(response.content)

            html_content = response.text
            save_html_pages(html_content,html_pages)
            s = Selector(text=response.text)

            product_name = s.xpath('//h4[@class="sc-fuzEkO hGFQxd"]/text()').get().strip()
            # print(product_name)

            product_price = s.xpath('//div[@class="sc-17hyc2s-3 jOoliK sc-nUItV fwvCZw"]/span/text()').getall()
            product_price = ', '.join(product_price)
            # print(product_price)

            product_desc = s.xpath('//p[@class="sc-cGCqpu bWHESZ"]/text()').get()
            # print(product_desc)

            # img_url_list = s.xpath("//div[contains(@class,'product-gallery__image')]/a/img/@src").getall()
            img_url_list = s.xpath('//img[@class="sc-s1isp7-5 fyZwWD"]/@src').getall()
            img_url = ", ".join(img_url_list)
            # print(img_url)

# Product_name, Product_id, Product_price, Product_desc, Img_url, Category, Subcategory, Brand, Related_Product, Product_url, Varients, Status
            print(f"Status code : {response.status_code},{url} insert in db.")
            return (url,product_name,product_price,product_desc, img_url)

    except Exception as e:
        print("Error scraping:", e)
        return None

def save_html_pages(s, html_pages):
    directory = "C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\HTML_PDP_PAGES"
    os.makedirs(directory, exist_ok=True)

    # filename = directory + "\\" + f"page_{html_page}.html"
    filename = os.path.join(directory, f"{html_pages}.html")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(s)

    print("Saved HTML file:", filename)

# Main Function
def main():
    product_urls = fetch_product_urls()
    if not product_urls:
        print("No product URLs found.")
        return
    scraped_data = []
    for url,product_id in product_urls:
        details = scrape_product_details(url,html_pages)
        if details:
            scraped_data.append(details)

    if scraped_data:
        insert_product_details(scraped_data)
        data_in_excel()
        print("Process Completed!")

# Insert in database
def insert_product_details(data):
    conn, cursor = database()

    try:
        # url,product_name,product_price,product_desc, img_url
        query = """
            INSERT INTO Zoja_pdp_page(Product_url,Product_name,Product_price,Product_desc,Img_url,Status)
            VALUES (%s, %s, %s, %s, %s, 'Done')
        """

        for record in data:
            cursor.execute(query, record)
            conn.commit()
            print(f"Inserted: {record[0]}")

        # cursor.executemany(query, data)
        # conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print("Database Insertion Error:", e)

    finally:
        cursor.close()
        conn.close()


def data_in_excel():
    conn, cursor = database()
    try:
        table_name = "Zoja_pdp_page"
        file_name = f"{table_name}.xlsx"

        directory = f"C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\Zomato\\{file_name}"
        os.makedirs(directory, exist_ok=True)

        # if not os.path.exists(directory):
        #     os.makedirs(directory, exist_ok=True)

        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            # The first element (desc[0]) of each tuple is the column name.
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            df.to_excel(file_name, index=False)
            print("Excel file created successfully!")
            print("Insert in excel file!!")
        else:
            print("No data available to write to Excel.")

    except Exception as e:
        print("Excel file error:", e)
    finally:
        cursor.close()
        conn.close()

main()
