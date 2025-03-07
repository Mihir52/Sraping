from math import trunc

import requests
import pymysql
import os
from lxml import html
import pandas as pd
from parsel import Selector


cookies = {
    '_ga': 'GA1.1.715542908.1741326855',
    'sbjs_migrations': '1418474375998%3D1',
    'sbjs_current_add': 'fd%3D2025-03-07%2005%3A24%3A15%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.zoja.ae%2F%3Fs%3Dshampoo%26post_type%3Dproduct%7C%7C%7Crf%3D%28none%29',
    'sbjs_first_add': 'fd%3D2025-03-07%2005%3A24%3A15%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.zoja.ae%2F%3Fs%3Dshampoo%26post_type%3Dproduct%7C%7C%7Crf%3D%28none%29',
    'sbjs_current': 'typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29',
    'sbjs_first': 'typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29',
    'sbjs_udata': 'vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F133.0.0.0%20Safari%2F537.36',
    'lightbox_header-newsletter-signup': 'opened_1',
    'crisp-client%2Fsession%2Fb980bca2-39a8-47cf-805c-994012db95d5': 'session_96e1da39-3380-486f-9504-e4d7d58e9b45',
    'sbjs_session': 'pgs%3D9%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.zoja.ae%2Fpage%2F1%2F%3Fs%3Dshampoo%26post_type%3Dproduct',
    '_ga_N98HPHQPDM': 'GS1.1.1741326855.1.1.1741327246.55.0.0',
    '_ga_D9NR8SKS13': 'GS1.1.1741326855.1.1.1741327246.0.0.0',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.zoja.ae/page/2/?s=shampoo&post_type=product',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    # 'cookie': '_ga=GA1.1.715542908.1741326855; sbjs_migrations=1418474375998%3D1; sbjs_current_add=fd%3D2025-03-07%2005%3A24%3A15%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.zoja.ae%2F%3Fs%3Dshampoo%26post_type%3Dproduct%7C%7C%7Crf%3D%28none%29; sbjs_first_add=fd%3D2025-03-07%2005%3A24%3A15%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.zoja.ae%2F%3Fs%3Dshampoo%26post_type%3Dproduct%7C%7C%7Crf%3D%28none%29; sbjs_current=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29; sbjs_first=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29; sbjs_udata=vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F133.0.0.0%20Safari%2F537.36; lightbox_header-newsletter-signup=opened_1; crisp-client%2Fsession%2Fb980bca2-39a8-47cf-805c-994012db95d5=session_96e1da39-3380-486f-9504-e4d7d58e9b45; sbjs_session=pgs%3D9%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.zoja.ae%2Fpage%2F1%2F%3Fs%3Dshampoo%26post_type%3Dproduct; _ga_N98HPHQPDM=GS1.1.1741326855.1.1.1741327246.55.0.0; _ga_D9NR8SKS13=GS1.1.1741326855.1.1.1741327246.0.0.0',
}

params = {
    's': 'shampoo',
    'post_type': 'product',
}

response = requests.get('https://www.zoja.ae/page/1/', params=params, cookies=cookies, headers=headers)

def database_creation():
    try:
        db = pymysql.connect(host="localhost", user='root', password="xbyte")
        cursor = db.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS Mock_test_3")
        db.commit()
    except Exception as e:
        print("database_creation****", e)

    try:
        db = pymysql.connect(host="localhost", user="root", password="xbyte", database="Mock_test_3")
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Zoja_PL_Page(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_id VARCHAR(50) NOT NULL,
                Product_url TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID))
            """)

        db.commit()
    except Exception as e:
        print("Table creation****", e)

    return db, cursor


def main_page():
    try:
        db, cursor = database_creation()
        # https: // www.zoja.ae / page / 1 /?s = shampoo & post_type = product
        url = "https://www.zoja.ae/"
        pages = 1
        all_data = []
        html_page = 1

        while pages:
            try:
                res = requests.get(f"{url}/page/{pages}/?s=shampoo&post_type=product", headers=headers, cookies=cookies, params=params)
                # print(f"Fetching : {url}&page={pages} , Status code : {res.status_code}")
                print(f"{res},Status code : {res.status_code}")

                if res.status_code == 200:

                    save_html_pages(res.text, html_page)

                    s = Selector(text=res.text)
                    # tree = html.fromstring(res.content)

                    product_id = s.xpath('//div[@class="add-to-cart-button"]/a/@data-product_id').getall()
                    print(product_id)

                    product_url = s.xpath("//*[contains(@class,'product-title')]/a/@href").getall()
                    print(product_url)

                    all_data.extend(zip(product_id, product_url))

                    # next_page = tree.xpath("//a[contains(@class, 's-pagination-next')]/@href")
                    next_page = s.xpath('//a[@class="next page-number"]/@href').getall()
                    if next_page:
                        pages += 1
                        # print(pages)
                        html_page += 1
                        # f"{url}_pg_{pages}"
                    else:
                        break

                else:
                    print(f"Response : {res.status_code}")

            except Exception as e:
                print("Pages*********", e)

        if all_data:
            data_store = pd.DataFrame(all_data, columns=['Product_id', 'Product_url'])
            data_in_excel(data_store)
            insert_into_database(db, cursor, all_data)

        else:
            print("No store data!")

    except Exception as e:
        print("*********", e)


def save_html_pages(s, html_page):
    directory = "C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\HTML_PL_PAGES"
    os.makedirs(directory, exist_ok=True)  # Ensure the directory exists

    filename = directory + "\\" + f"page_{html_page}.html"  # Now directory is a valid string
    with open(filename, "w", encoding="utf-8") as f:
        f.write(s)

    print("Saved HTML file:", filename)

def data_in_excel(data_store):
    try:
        data_store.to_excel("C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\PL_PAGE.xlsx", index=False)
        print("Successfully insert in Excel.")

    except Exception as e:
        print("****Excel data store*****", e)

def insert_into_database(db, cursor, data):
    try:
        for row in data:
            query = """
                INSERT INTO Zoja_PL_Page(Product_id, Product_url, Status)
                VALUES (%s, %s, 'Done')
            """
            cursor.execute(query, row)

        db.commit()
        db.close()
        print("Data Successfully Inserted into Database")

    except Exception as e:
        print("Database Insertion Error:", e)

main_page()



