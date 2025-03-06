import requests, pymysql, os, json
from parsel import Selector
import hashlib
import unicodedata
import html, re
from threading import Thread
import pandas as pd

def databse():
    try:
        db = pymysql.connect(host="localhost",user='root',password="xbyte")
        cursor = db.cursor()

        # cursor.execute("CREATE DATABASE IF NOT EXISTS Purplle_web")
        db.commit()
    except Exception as e:
        print("connection error****",e)
    
    try:
        db = pymysql.connect(host="localhost",user="root",password="xbyte",database="Purplle_web")
        cursor = db.cursor()
        db.commit()
    except Exception as e:
        print("Table creation****",e)
    
    return db,cursor

def fetch_product_urls():
    conn, cursor = databse()
    try:
        query = "SELECT Product_url FROM Lip_care_PL"  
        cursor.execute(query)
        urls = cursor.fetchall()
        urls = [url[0] for url in urls]
        # print(len(urls))
        return urls

    except Exception as e:
        print("Error fetching product URLs:", e)

    finally:
        cursor.close()
        conn.close()


def scrape_product_details(url,page_number):
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            s = Selector(text=response.text)

            script_tag = s.xpath('//script[@type="application/ld+json"]/text()').getall()
            # print(script_tag)

            # Make dir to save pages
            directory = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Purplle_web\\Shop_Category\\Skin_care\\LipCare\\Complete_lip_care_collection\\Lipcare_pdp"
            os.makedirs(directory,exist_ok=True)
            
            # First html page save
            html_file = os.path.join(directory,f"Page_{page_number}.html")
            with open(html_file,'w',encoding="utf-8") as f:
                f.write(response.text)

            print(f"Saved html file : {page_number}")

            # secondly save json file
            if script_tag:
                json_data = script_tag[0]  # Get the first script tag content
                json_file = os.path.join(directory,f"Page_{page_number}.json")

                with open(json_file,'w',encoding="utf-8") as f:
                    f.write(json_data)
                print(f"Saved json file : {page_number}\n")

            return True

            # print(f"{url} insert in db.")
            # return (url,product_name,product_id,product_price,product_desc,img_url,category,subcategory,related_products_str)
            

    except Exception as e:
        print("Error scraping:", e)
        return None

def main():
    product_urls = fetch_product_urls()
    for i,url in enumerate(product_urls,start=1):
        scrape_product_details(url,i)

    # print(detais)
main()


    
    


