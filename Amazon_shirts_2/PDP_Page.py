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

cookies = {
    'session-id': '145-3785797-7207240',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'USD',
    'ubid-main': '135-5865474-6046062',
    'lc-main': 'en_US',
    'skin': 'noskin',
    'sp-cdn': '"L5Z9:IN"',
    'session-token': 'So5YTX0zMsTM0RIuVaN87q0JoRw/grwLqix4y004+0pUIbgpPlqtnezUfVu4ORYk7cswf7xMKQkLa0QIH0mw7WUcfYElgLiqWWDdat4xH4CO6f0Mjja78WfxvL3QgGigDxgtc9pT19gOONwpc3GHZfq127XlHzeTpHwdl7ATCrubgUeCb4OSMrRq2GT1No9P9ye+V82k26zNID6eE50tkCqlIkPDFbf+BgkdT9ZcilGxxkqA7SslTHqIRBN5dnsxjv8i9MwSLwvjYa+RFvCl3TmgZXGKyhJc6C0NxfWOEqj5rGP/YqCHaOlOF9enSNQkn4QPw8Utc0pUsvb0MbhdnmPWs7tCI1PF',
    'csm-hit': 'tb:s-6YA7S2XDBP1010GYF2XW|1741250663935&t:1741250666916&adb:adblk_no',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'device-memory': '8',
    'downlink': '1.55',
    'dpr': '1',
    'ect': '3g',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.amazon.com/s?k=shirts+for+men&crid=68I51CZR7NYT&sprefix=shirts%2Caps%2C751&ref=nb_sb_ss_ts-doa-p_3_6',
    'rtt': '350',
    'sec-ch-device-memory': '8',
    # 'sec-ch-dpr': '1',
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
    # 'cookie': 'session-id=145-3785797-7207240; session-id-time=2082787201l; i18n-prefs=USD; ubid-main=135-5865474-6046062; lc-main=en_US; skin=noskin; sp-cdn="L5Z9:IN"; session-token=So5YTX0zMsTM0RIuVaN87q0JoRw/grwLqix4y004+0pUIbgpPlqtnezUfVu4ORYk7cswf7xMKQkLa0QIH0mw7WUcfYElgLiqWWDdat4xH4CO6f0Mjja78WfxvL3QgGigDxgtc9pT19gOONwpc3GHZfq127XlHzeTpHwdl7ATCrubgUeCb4OSMrRq2GT1No9P9ye+V82k26zNID6eE50tkCqlIkPDFbf+BgkdT9ZcilGxxkqA7SslTHqIRBN5dnsxjv8i9MwSLwvjYa+RFvCl3TmgZXGKyhJc6C0NxfWOEqj5rGP/YqCHaOlOF9enSNQkn4QPw8Utc0pUsvb0MbhdnmPWs7tCI1PF; csm-hit=tb:s-6YA7S2XDBP1010GYF2XW|1741250663935&t:1741250666916&adb:adblk_no',
}

params = {
    'crid': '68I51CZR7NYT',
    # 'dib': 'eyJ2IjoiMSJ9.S-66RIJN6Z8JrsjYP1KQ9UFSvpNtj04aHaQsPpl029LuEUqRq5djdiGK1IxOs1ym6tYXupWlAsBGVnGCcXLSTZGK--309_nCV2Ggy2-MBDXI2vPUXpz-CQtH3cX_hXDKuD-3jemBfbTly7Z9C_mY0RJHhoMPxlyaMZJge5o73TtthszoF16ozssAV-xhFCQPNZ2M2oEi1USBc_7neVHEEPl1Rdb8aeBuhhtvY25oeUqzf-2_DveMMcyDBgm_KWDPgnGzM0mwDLsK9DLWtF97yE41_j9XD1FBNcEKQKx42qU.sjPZ__qQgvmUvJZ_GF31yW0ol2Mw6X08iPev5rF-uz4',
    'dib_tag': 'se',
    'keywords': 'shirts for men',
    'qid': '1741250649',
    'sprefix': 'shirts,aps,751',
    'sr': '8-1-spons',
    'sp_csd': 'd2lkZ2V0TmFtZT1zcF9hdGY',
    'psc': '1',
}


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
                Product_name TEXT;
                Product_color TEXT,
                Product_Size TEXT,
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
        response = requests.get(url,headers=headers,cookies=cookies,params=params)
        print(f"Status code : {response.status_code} , {url}")

        if response.status_code == 200:
            save_html_pages(response.text,product_id)
            # tree = html.fromstring(response.content)

            s = Selector(text=response.text)

            # product_name = s.xpath('//span[@id="productTitle"]/text()').get().strip()
            # print(product_name)
            product_name = s.xpath('//span[@id="productTitle"]/text()').get().strip()
            product_name = product_name[:500]

            product_colors = []
            color_ids = s.xpath("//li[contains(@id,'color_name')]/@data-defaultasin").getall()
            # color_names = s.xpath("//li[contains(@id,'color_name')]/@title").getall()  # Extracts color names

            for color_id in (color_ids):
                color_page_url = f"https://www.amazon.com/dp/{color_id}"

                color_response = requests.get(color_page_url, headers=headers, cookies=cookies)

                if color_response.status_code == 200:
                    color_selector = Selector(text=color_response.text)
                    extracted_color = color_selector.xpath('//span[@class="selection"]/text()').get()

                    if extracted_color:
                        product_colors.append(extracted_color.strip())
                    # else:
                        # Fallback to extracting from the title attribute
                        # cleaned_color_name = color_name.replace("Click to select ", "").strip()
                        # product_colors.append(cleaned_color_name)
            # print("Colors:", product_colors)


            size_name = s.xpath('//option[@data-a-css-class="dropdownAvailable"]/text()').getall()
            # size_name = ' | '.join(size_name)
            # print("sizes : ",size_name)

            scraped_rows = []
            for color in product_colors:
                scraped_rows.append((url,product_id,product_name,product_colors,size_name))
            return scraped_rows

    except Exception as e:
        print("Error scraping:", e)
        return None

def save_html_pages(s, product_id):
    directory = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Amazon_shirts_2\\PDP_Pages"
    os.makedirs(directory, exist_ok=True)

    # filename = directory + "\\" + f"page_{html_page}.html"
    filename = os.path.join(directory, f"{product_id}.html")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(s)

    print("Saved HTML file:", filename)


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
            insert_product_details(details)
            scraped_data.append(details)

    if scraped_data:

        data_in_excel()
        print("Process Completed!")

# Insert into db
def insert_product_details(data):
    conn, cursor = database()
    try:
        query = """
            INSERT INTO PDP_PAGE (Product_id,Product_url,Product_name,Product_color,Product_Size,Status)
            VALUES (%s, %s, %s, %s, %s,'Done')
        """
        normalized_data = []
        for p_id,p_url,p_name,p_color,p_size in data:
            if p_color:
                for c in p_color:
                    # cursor.execute(query,(p_id,p_url,p_name,c,None))   # one by one data insert in db
                    # conn.commit()

                    normalized_data.append((p_id,p_url,p_name,c,None))   #whole data insert in db

            if p_size:
                for s in p_size:
                    # cursor.execute(query,(p_id,p_url,p_name,None,s))   #one by one data insert in db
                    # conn.commit()
                    normalized_data.append((p_id,p_url,p_name,None,s))  #whole data insert in db

        # print(normalized_data)
        cursor.executemany(query, normalized_data)
        conn.commit()
        print("Data inserted successfully in db!")

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
