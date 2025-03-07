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

import requests

cookies = {
    '_ga': 'GA1.1.715542908.1741326855',
    'sbjs_migrations': '1418474375998%3D1',
    'sbjs_current_add': 'fd%3D2025-03-07%2005%3A24%3A15%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.zoja.ae%2F%3Fs%3Dshampoo%26post_type%3Dproduct%7C%7C%7Crf%3D%28none%29',
    'sbjs_first_add': 'fd%3D2025-03-07%2005%3A24%3A15%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.zoja.ae%2F%3Fs%3Dshampoo%26post_type%3Dproduct%7C%7C%7Crf%3D%28none%29',
    'sbjs_current': 'typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29',
    'sbjs_first': 'typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29',
    'lightbox_header-newsletter-signup': 'opened_1',
    'mailchimp_landing_site': 'https%3A%2F%2Fwww.zoja.ae%2Fitem%2FJL-4154%2F',
    '_ga_N98HPHQPDM': 'deleted',
    'woocommerce_recently_viewed': '22373%7C26136%7C25997%7C9673%7C17738',
    'sbjs_udata': 'vst%3D2%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F133.0.0.0%20Safari%2F537.36',
    'sbjs_session': 'pgs%3D7%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.zoja.ae%2Fbrand%2Fican-london%2F',
    '_ga_N98HPHQPDM': 'GS1.1.1741332292.2.1.1741333790.53.0.0',
    'crisp-client%2Fsession%2Fb980bca2-39a8-47cf-805c-994012db95d5': 'session_5185ae24-747a-4e0c-baa6-cb52717a8ec6',
    '_ga_D9NR8SKS13': 'GS1.1.1741332292.2.1.1741334340.0.0.0',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.zoja.ae/page/1/?s=shampoo&post_type=product',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}


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
            CREATE TABLE IF NOT EXISTS Zoja_pdp_page(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_name TEXT,
                Product_id VARCHAR(255) NOT NULL,
                Product_price VARCHAR(50),
                Product_desc TEXT,
                Img_url TEXT,
                Category TEXT,
                Subcategory TEXT,
                Brand TEXT,
                Related_Product TEXT,
                Product_url TEXT,
                Varients TEXT,
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
        query = "SELECT Product_url,Product_id FROM Zoja_PL_Page"
        cursor.execute(query)
        urls = cursor.fetchall()
        # urls = [url[0] for url in urls]
        # print(urls)
        return urls

    except Exception as e:
        print("Error fetching product URLs:", e)

    finally:
        cursor.close()
        conn.close()

# fetch inner product details through product url
def scrape_product_details(url,product_id):
    try:
        response = requests.get(url,headers=headers,cookies=cookies)
        # print(f"Product ID: {product_id}, Length: {len(product_id)}")

        if response.status_code == 200:
            tree = html.fromstring(response.content)

            html_content = response.text
            save_html_pages(html_content, product_id)
            s = Selector(text=response.text)

            product_name = s.xpath("//h1[contains(@class,'product-title')]/text()").get().strip()
            # print(product_name)

            product_price = s.xpath("//div[contains(@class,'product-info')]//span/bdi[1]/text()").getall()
            product_price = ', '.join(product_price)
            # print(product_price)

            product_desc = s.xpath('//div[@id="tab-description"]/p/text()').get()
            # print(product_desc)

            # img_url_list = s.xpath("//div[contains(@class,'product-gallery__image')]/a/img/@src").getall()
            img_url_list = s.xpath("//div[contains(@class, 'woocommerce-product-gallery__image')]//a/@href").getall()
            img_url = ", ".join(img_url_list)
            # print(img_url)

            # category = s.xpath("//nav[@class='woocommerce-breadcrumb']//a[2]/text()")
            category = s.xpath('//nav[@id="breadcrumbs"]/span/a[2]/text()').get()
            # print(category)

            subcategory = s.xpath('//nav[@id="breadcrumbs"]/span/a[3]/text()').get()
            # print(subcategory)

            brand = s.xpath('//a[@title="View brand"]/text()').get()
            # print(brand)

            related_product = s.xpath("//p[contains(@class,'product-title')]/a/text()").getall()
            related_products_str = ", ".join(related_product)
            # print(related_products_str)

            varients = s.xpath('//div[@class="variable-item-contents"]/span/text()').getall()
            varients = ', '.join(varients)
            # print(varients)

# Product_name, Product_id, Product_price, Product_desc, Img_url, Category, Subcategory, Brand, Related_Product, Product_url, Varients, Status
            print(f"Status code : {response.status_code},{url} insert in db.")
            return (product_name,product_id,product_price,product_desc, img_url, category, subcategory,brand,
                    related_products_str,url,varients)


    except Exception as e:
        print("Error scraping:", e)
        return None

def save_html_pages(s, product_id):
    directory = "C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\HTML_PDP_PAGES"
    os.makedirs(directory, exist_ok=True)

    # filename = directory + "\\" + f"page_{html_page}.html"
    filename = os.path.join(directory, f"{product_id}.html")
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
        details = scrape_product_details(url,product_id)
        if details:
            scraped_data.append(details)

    if scraped_data:
        insert_product_details(scraped_data)
        data_in_excel()
        print("Process Completed!")

# Insert in database
# def insert_product_details(data):
#     conn, cursor = database()
#
#     try:
#         query = """
#             INSERT INTO Zoja_pdp_page(Product_name,Product_id,Product_price,Product_desc,Img_url,Category,Subcategory,Brand,Related_Product,Product_url,Varients,Status)
#             VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Done')
#         """
#
#         for record in data:
#             cursor.execute(query, record)
#             conn.commit()
#             print(f"Inserted: {record[0]}")
#
#         # cursor.executemany(query, data)
#         # conn.commit()
#         print("Data inserted successfully!")
#
#     except Exception as e:
#         print("Database Insertion Error:", e)
#
#     finally:
#         cursor.close()
#         conn.close()

def insert_product_details(data):
    conn, cursor = database()

    try:
        query = """
            INSERT INTO Zoja_pdp_page(
                Product_name, Product_id, Product_price, Product_desc, Img_url, 
                Category, Subcategory, Brand, Related_Product, Product_url, Varients, Status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Done')
        """
        for record in data:
            product_name, product_id, product_price, product_desc, img_url, category,subcategory, brand, related_products_str, url, varients = record

            variant_list = varients.split(", ") if varients else [None]

            for variant in variant_list:
                cursor.execute(query, (
                    product_name, product_id, product_price, product_desc, img_url,
                    category, subcategory, brand, related_products_str, url, variant
                ))
                conn.commit()
                print(f"Inserted: {product_name} - Variant: {variant}")

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

        directory = f"C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\{file_name}.xlsx"
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
