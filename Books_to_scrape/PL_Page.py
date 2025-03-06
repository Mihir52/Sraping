# Product Listing page

import requests
from pymongo import MongoClient
import pandas as pd
from lxml import html
import pymysql
 

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'if-modified-since': 'Wed, 08 Feb 2023 21:02:32 GMT',
    'if-none-match': 'W/"63e40de8-c85e"',
    'priority': 'u=0, i',
    'referer': 'https://www.google.com/',
    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
}

response = requests.get('https://books.toscrape.com/', headers=headers)
# https://books.toscrape.com/
# https://books.toscrape.com/catalogue/page-2.html
def product_list_fetch():
    try:
        url = "https://books.toscrape.com"
        pages = 1
        all_data = []

        while pages:
            try:
                # res = requests.get(url + pages)
                # print(f"{url + pages}, Status code: {res.status_code}")

                res = requests.get(f"{url}/catalogue/page-{pages}.html",headers=headers)
                print(f"Fetching : {url}/catalogue/page-{pages}.html , Status code : {res.status_code}")
                
                if res.status_code == 200:
                    tree = html.fromstring(res.content)

                    book_title = tree.xpath('//li[@class="col-xs-6 col-sm-4 col-md-3 col-lg-3"]//h3/a/text()')
                    # print(len(book_title))

                    book_price = []
                    for p in tree.xpath('//p[@class="price_color"]/text()'):
                        # £51.77
                        valid_price = p.replace('£','')
                        book_price.append(valid_price)
                    # print(len(book_price))
                    
                    
                    book_in_stock = []
                    
                    for i,b in enumerate(tree.xpath("//p[@class='instock availability']/text()")):
                        if i%2==1:
                            valid_stock = b.strip()
                            book_in_stock.append(valid_stock)
                    # print(book_in_stock)

                    # zip combine multiple list element
                    all_data.extend(zip(book_title, book_price, book_in_stock))

                    # next page URL
                    next_page = tree.xpath('//ul[@class="pager"]/li[@class="next"]/a/@href')
                    if next_page:
                        # pages = next_page[0]
                        pages += 1
                    else:
                        break

                else:
                    print(f"{res.status_code}")
                    break  

            except:
                print(f"request {res.status_code}")
                break  

        if all_data:
            product_list_data = pd.DataFrame(all_data, columns=["book_title", "book_price", "book_in_stock"])
            data_in_excel(product_list_data)
            data_store_database(product_list_data)
            # data_store_sqlyog(product_list_data)
        else:
            print("not store data")

    except:
        print(f"Status code : {res.status_code}")

# Data store in excel
def data_in_excel(product_list_data):
    try:
        product_list_data.to_excel("C:\\Users\\mihir.parate\\Desktop\\Python task\\Books_to_scrape\\Book_list.xlsx", index=False)
        print("Successfully Save in excel.")
    except Exception as e:
        print(f"data not saving!!",e)


# Data store in database
def data_store_database(product_list_data):
    try:
        client = MongoClient("mongodb://admin:tP_kc8mn@localhost:27017/?authSource=admin")
        db = client['Book_to_scrape']
        collection = db['Product_list']

        collection.insert_many(product_list_data.to_dict(orient='records'))
        print("Data stored successfully in MongoDB")
    except Exception as e:
        print("Database error!!")
        

# Data store in Sqlyog
# def data_store_sqlyog(product_list_data):
#     try:
#         db = pymysql.connect(host="localhost",user="root",passwd="xbyte")
#         cursor = db.cursor()

#         cursor.execute("CREATE DATABASE IF NOT EXISTS QUOTE_TO_SCRAPE")
#         db.commit()

#         db = pymysql.connect(host="localhost", user="root", password="xbyte", database="QUOTE_TO_SCRAPE")
#         cursor = db.cursor()



#         cursor.execute('''
#             CREATE TABLE IF NOT EXISTS data(
#                 Quote TEXT NOT NULL,
#                 Author TEXT NOT NULL,
#                 Tags TEXT NOT NULL)
#         ''')

#         db.commit()

#         cursor.execute("INSERT INTO data (Quote,Author,Tags) VALUES (%s, %s, %s)",(product_list_data['Quote'],product_list_data['Author'],product_list_data['Tags'])) 

#         db.commit()

#         print("Data added in database(sqlyog)")

#         db.close()
#     except Exception as e:
#         print("***************",e)

product_list_fetch()