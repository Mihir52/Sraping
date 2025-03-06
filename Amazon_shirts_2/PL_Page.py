import requests
import pymysql
import os
from lxml import html
import pandas as pd
from parsel import Selector

import requests

cookies = {
    'session-id': '145-3785797-7207240',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'USD',
    'ubid-main': '135-5865474-6046062',
    'lc-main': 'en_US',
    'skin': 'noskin',
    'sp-cdn': '"L5Z9:IN"',
    'session-token': 'PJgkQ/4HWaRDLtC5bBgEUfHWR+eJygv6ghmzyl6J9hfUFZFI8Rp3IpsJ0rZQXMA5LZNTiCIMPDMv/E8YwPOZO3HSKE7cARIKldEzQinfN1ziZhGVykahx9k+hBmFAI8wKVfPIJcUO1sZXEkMRUqBJ4N5bKbkFiV8cFP9uj2JZ8X8malS1VJauPjmeN5nBvLRvmiI1N6f7yZV4CnrdiCPhI/K/z2He/MK+UcpXKVK5D4GIjNJo2dVkDKDpX661pSNQ9rzcm1OOrxhe7+Ox4CvcynseUFWcJdwO+DxdwbDwIWmjX7/l7YVcNQtjAUV472qbZd7HRi9aBYTqwF9L2RBmM6/iIA8D7ia',
    'csm-hit': 'tb:QJEGHRMGVS5366EK5NBE+s-W08EB5P2YJ8Q0GG15145|1741235802104&t:1741235802104&adb:adblk_no',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'device-memory': '8',
    'downlink': '10',
    'dpr': '1',
    'ect': '4g',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'rtt': '50',
    'sec-ch-device-memory': '8',
    'sec-ch-dpr': '1',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-ch-viewport-width': '1920',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'viewport-width': '1920',
    # 'cookie': 'session-id=145-3785797-7207240; session-id-time=2082787201l; i18n-prefs=USD; ubid-main=135-5865474-6046062; lc-main=en_US; skin=noskin; sp-cdn="L5Z9:IN"; session-token=PJgkQ/4HWaRDLtC5bBgEUfHWR+eJygv6ghmzyl6J9hfUFZFI8Rp3IpsJ0rZQXMA5LZNTiCIMPDMv/E8YwPOZO3HSKE7cARIKldEzQinfN1ziZhGVykahx9k+hBmFAI8wKVfPIJcUO1sZXEkMRUqBJ4N5bKbkFiV8cFP9uj2JZ8X8malS1VJauPjmeN5nBvLRvmiI1N6f7yZV4CnrdiCPhI/K/z2He/MK+UcpXKVK5D4GIjNJo2dVkDKDpX661pSNQ9rzcm1OOrxhe7+Ox4CvcynseUFWcJdwO+DxdwbDwIWmjX7/l7YVcNQtjAUV472qbZd7HRi9aBYTqwF9L2RBmM6/iIA8D7ia; csm-hit=tb:QJEGHRMGVS5366EK5NBE+s-W08EB5P2YJ8Q0GG15145|1741235802104&t:1741235802104&adb:adblk_no',
}

params = {
    'k': 'shirts for men',
}

response = requests.get('https://www.amazon.com/s', params=params, cookies=cookies, headers=headers)

def database_creation():
    try:
        db = pymysql.connect(host="localhost", user='root', password="xbyte")
        cursor = db.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS Amazon_shirts_2")
        db.commit()
    except Exception as e:
        print("database_creation****", e)

    try:
        db = pymysql.connect(host="localhost", user="root", password="xbyte", database="Amazon_shirts_2")
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PL_Page(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_id VARCHAR(50) NOT NULL,
                Product_url TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID)
            )
        """)

        db.commit()
    except Exception as e:
        print("Table creation****", e)

    return db, cursor


def main_page():
    try:
        db, cursor = database_creation()
        # https://www.amazon.com/s?k=shirts+for+men&page=2
        # https://www.amazon.com/s?k=shirts+for+men&page=2&qid=1741235583&xpid=WUcXmOU5rYMdr&ref=sr_pg_2
        url = "https://www.amazon.com/s?k=shirts+for+men"
        pages = 1
        all_data = []
        html_page = 1

        while pages:
            try:
                res = requests.get(f"{url}&page={pages}", headers=headers, cookies=cookies, params=params)
                print(f"Fetching : {url}&page={pages} , Status code : {res.status_code}")

                if res.status_code == 200:

                    save_html_pages(res.text, html_page)

                    s = Selector(text=res.text)
                    # tree = html.fromstring(res.content)

                    product_id = s.xpath("//div[contains(@class,'puis-card-container')]/@data-dib-asin").getall()
                    # print(product_id)

                    # product_url = s.xpath("//div[contains(@class,'puis-card-container')]/@data-dib-asin")
                    product_url = []
                    # https://www.amazon.com/dp/B07D37PQGL
                    for p_url in s.xpath("//div[contains(@class,'puis-card-container')]/@data-dib-asin").getall():
                        if not p_url.startswith('https://www.amazon.com/dp/'):
                            valid_url = f"https://www.amazon.com/dp/{p_url}"
                            product_url.append(valid_url)
                        else:
                            pass

                    # product_price = tree.xpath('//span[@class="a-price-whole"]/text()')

                    # img_url = tree.xpath('//img[@class="s-image"]/@src')
                    # product_price = tree.xpath('//span[@class="a-price-whole"]/text()')

                    # product_rating = []
                    # # 4.7 out of 5 stars
                    # for p_rating in tree.xpath('//span[@class="a-icon-alt"]/text()'):
                    #     valid_rating = p_rating.replace(p_rating[4:],'')
                    #     product_rating.append(valid_rating)

                    all_data.extend(zip(product_id, product_url))

                    # next_page = tree.xpath("//a[contains(@class, 's-pagination-next')]/@href")
                    next_page = s.xpath("//a[contains(@class,'s-pagination-next')]/@href").getall()
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
    directory = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Amazon_shirts_2\\Html_pages"
    os.makedirs(directory, exist_ok=True)  # Ensure the directory exists

    filename = directory + "\\" + f"page_{html_page}.html"  # Now directory is a valid string
    with open(filename, "w", encoding="utf-8") as f:
        f.write(s)

    print("Saved HTML file:", filename)


def data_in_excel(data_store):
    try:
        data_store.to_excel("C:\\Users\\mihir.parate\\Desktop\\Python task\\Amazon_shirts_2\\PL_Page.xlsx", index=False)
        print("Successfully insert in Excel.")

    except Exception as e:
        print("****Excel data store*****", e)


def insert_into_database(db, cursor, data):
    try:
        for row in data:
            query = """
                INSERT INTO PL_Page (Product_id, Product_url, Status)
                VALUES (%s, %s, 'Done')
            """
            cursor.execute(query, row)

        db.commit()
        db.close()
        print("Data Successfully Inserted into Database")

    except Exception as e:
        print("Database Insertion Error:", e)


main_page()



