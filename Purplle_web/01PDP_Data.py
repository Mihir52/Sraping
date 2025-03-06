import requests, pymysql, os, json
from parsel import Selector
import hashlib
import unicodedata
import html, re
from threading import Thread
import pandas as pd

def database():
    try:
        db = pymysql.connect(host="localhost",user='root',password="xbyte")
        cursor = db.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS Purplle_web")
        db.commit()
    except Exception as e:
        print("database_creation****",e)
    
    try:
        db = pymysql.connect(host="localhost",user="root",password="xbyte",database="Purplle_web")
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Lip_care_Pdp(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_id VARCHAR(50) NOT NULL,
                Product_url TEXT,
                Product_name TEXT,
                Product_price VARCHAR(50),
                Thumb_img_url TEXT,
                Description TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID)
            )
        """)
        db.commit()
    except Exception as e:
        print("Table creation****",e)
    
    return db,cursor


def fetch_product_ids():
    conn, cursor = database()
    try:
        query = "SELECT Product_id FROM lip_care_pl"  
        cursor.execute(query)
        p_id = cursor.fetchall()
        # print(id)    
        return [p[0] for p in p_id]

    except Exception as e:
        print("Error fetching product URLs:", e)

    finally:
        cursor.close()
        conn.close()


def fetch_data(product_id):
    file_path = r"C:\\Users\\mihir.parate\\Desktop\\Python task\\Purplle_web\\Shop_Category\\Skin_care\\LipCare\\Complete_lip_care_collection\\Lipcare_pdp"
    
    all_data = []
    for file_name in sorted(os.listdir(file_path)):
        if file_name.endswith(".json"):
            full_path = os.path.join(file_path,file_name)
            print("\n",file_name)

            # read file
            with open(full_path,'r',encoding="utf-8") as f:
                data = json.load(f)

            product_url = data.get("url")
            # print(product_url)
            
            product_name = data.get('name')
            # print(product_name)

            product_desc = data.get('description')
            # print(product_desc)

            thumb_img = data.get('image')
            # print(thumb_img)

            # aggregateRating.ratingValue
            # for i in data.get("aggregateRating"):
            #     rating = i.get("ratingValue")
            # print(rating)
            
            rating = "N/A"
            aggregate_rating = data.get("aggregateRating")
            if isinstance(aggregate_rating, dict):
                rating = aggregate_rating.get("ratingValue", "N/A")
            # print(rating)    
            
            price = "N/A"
            offers = data.get("offers")
            if isinstance(offers, dict):
                price = offers.get("price", "N/A")
            # print(price)
            # offers.price
            # for p in data.get("offers"):
            #     price = i.get('price')
            #     print(price)
            
            if product_id and product_url and product_name and product_desc and thumb_img and rating and price:
                all_data.append((product_id,product_url,product_name,product_desc,thumb_img,rating,price))
                
    return all_data

# fetch_data()
def insert_into_database(data):
    conn,cursor = database()

    try:
        query = """INSERT INTO Lip_care_Pdp(Product_id, Product_url, Product_name,Product_price,Thumb_img_url,Description,Status) VALUES (%s, %s, %s, %s, %s, %s, 'Done')"""
        # print(query)

        cursor.executemany(query,data)
        conn.commit()

        # db.commit()
        # db.close()
        print("Data Successfully Inserted into Database")

    except Exception as e:
        print("Database Insertion Error:", e)
    finally:
        cursor.close()
        conn.close()
        
        
db,cursor = database()
p_ids = fetch_product_ids()
product_details = fetch_data(p_ids)
if product_details:
    insert_into_database(product_details)
db.close()

    