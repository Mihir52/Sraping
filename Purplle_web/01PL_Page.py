import json
import os
import requests
import pymysql
import requests

cookies = {
    'environment': 'prod',
    'is_robot': 'false',
    'mode_device': 'desktop',
    'is_webview': 'false',
    'lang_code': 'undefined',
    'vernac_ab_flag': 'undefined',
    'client_ip': '103.240.32.158',
    'visitorppl': '0kW9eGepTT0zZnpXCg',
    'device_id': '0kW9eGepTT0zZnpXCg',
    'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkZXZpY2VfaWQiOiIwa1c5ZUdlcFRUMHpabnBYQ2ciLCJtb2RlX2RldmljZSI6ImRlc2t0b3AiLCJtb2RlX2RldmljZV90eXBlIjoid2ViIiwiaWF0IjoxNzQwMTM1MjAxLCJleHAiOjE3NDc5MTEyMDEsImF1ZCI6IndlYiIsImlzcyI6InRva2VubWljcm9zZXJ2aWNlIn0.rj3saH6yJ4KH1b7_qvzZkHc0UIgLj8oHFhnm-LWErIY',
    'generic_visitorppl': 'KIwYcVvc80vWvBeuMJ1270011580205867',
    'generic_token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkZXZpY2VfaWQiOiJLSXdZY1Z2Yzgwdld2QmV1TUoxMjcwMDExNTgwMjA1ODY3IiwibW9kZV9kZXZpY2UiOiJkZXNrdG9wIiwibW9kZV9kZXZpY2VfdHlwZSI6IndlYiIsImlhdCI6MTU4NDA4NjYzOCwiZXhwIjoyNjkwNjQ3NTI0LCJhdWQiOiJ3ZWIiLCJpc3MiOiJ0b2tlbm1pY3Jvc2VydmljZSJ9.RdrqkTAPBDh0Qe-605a_dOYoXOOPcJe33f6tuMioKi8',
    'session_initiator': 'Direct',
    '__storage__modeDevice': 'desktop',
    '__storage__isRobot': 'false',
    '__storage__nc': '{"cc":0}',
    'isSessionDetails': 'true',
    '__storage__giftBox': '{"itemCount":0}',
    '__sessionstorage__giftBoxIcon': '0',
    '__storage__screen_ab_testing': '{"listing_ab_testing":[{"value":"d","types":"search,brand,category,landingpages","key":"ab_experiment_listing"}]}',
    '__storage__isEliteUser': 'false',
    '__storage__isLoggedIn': 'false',
    'is_first_session': 'false',
    '__sessionstorage__downloadStrip': 'true',
    'referrer': 'www.purplle.com',
    'utm_medium': 'referral',
    'utm_campaign': 'purplle-referral',
    'utm_source': 'Direct',
    'sessionCreatedTime': '1740719824',
    '__storage__fcm_time_stamp': '1740720600000',
    '__sessionstorage__PREV_PAGE_FROM_SESSION': '{"page_url":"https://www.purplle.com/skincare/lip-care","page_type":"listing_category","page_type_value":"11"}',
    'session_initiated': 'Direct',
    '__storage__supermenuExpiry': '1740722579986',
    'sessionExpiryTime': '1740724229',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded',
    'is_ssr': 'false',
    'mode_device': 'desktop',
    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjYxMjYxMTkiLCJhcCI6IjEzODYyMjgyMjIiLCJpZCI6IjA4NmJiZTFiNjc4MmY4MTMiLCJ0ciI6ImU5MWFlMTBhZTU0MThiYmY3ZTQ5YTRlMzNlMDk1NjMxIiwidGkiOjE3NDA3MjI0MzE2Nzl9fQ==',
    'ngsw-bypass': 'true',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.purplle.com/skincare/lip-care',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkZXZpY2VfaWQiOiIwa1c5ZUdlcFRUMHpabnBYQ2ciLCJtb2RlX2RldmljZSI6ImRlc2t0b3AiLCJtb2RlX2RldmljZV90eXBlIjoid2ViIiwiaWF0IjoxNzQwMTM1MjAxLCJleHAiOjE3NDc5MTEyMDEsImF1ZCI6IndlYiIsImlzcyI6InRva2VubWljcm9zZXJ2aWNlIn0.rj3saH6yJ4KH1b7_qvzZkHc0UIgLj8oHFhnm-LWErIY',
    'traceparent': '00-e91ae10ae5418bbf7e49a4e33e095631-086bbe1b6782f813-01',
    'tracestate': '6126119@nr=0-1-6126119-1386228222-086bbe1b6782f813----1740722431679',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'visitorppl': '0kW9eGepTT0zZnpXCg',
}

params = {
    'list_type': 'category',
    'list_type_value': 'skincare/lip-care',
    'page': '1',
    'sort_by': 'rel',
    'elite': '0',
    'custom': '',
    'mode_device': 'desktop',
}

# response = requests.get('https://www.purplle.com/neo/merch/listing', params=params, cookies=cookies, headers=headers)

def database_creation():
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

        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS shirts(
        #         ID INT NOT NULL AUTO_INCREMENT,
        #         Product_id VARCHAR(50) NOT NULL,
        #         Product_url TEXT,
        #         Img_url TEXT NOT NULL,
        #         Status VARCHAR(50) DEFAULT "Pending",
        #     )
        #     """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Lip_care_PL(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_id VARCHAR(50) NOT NULL,
                Product_url TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID)s
            )
        """)
        db.commit()
    except Exception as e:
        print("Table creation****",e)
    
    return db,cursor


def save_json_page():

    directory = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Purplle_web\\Shop_Category\\Skin_care\\LipCare\\Complete_lip_care_collection\\Lipcare_Json_pages"
    os.makedirs(directory,exist_ok=True)

    pages = 1
    
    while pages:    
        # api_url = "https://www.purplle.com/neo/merch/items/v2?tenant=PURPLLE_COM&sub_tenant=MAIN_SITE&userType=0&list_type=category&list_type_value=skincare/lip-care&page=1&sort_by=rel&elite=0&mode_device=desktop&identifier=0kW9eGepTT0zZnpXCg"

        res = requests.get(f"https://www.purplle.com/neo/merch/items/v2?tenant=PURPLLE_COM&sub_tenant=MAIN_SITE&userType=0&list_type=category&list_type_value=skincare/lip-care&page={pages}&sort_by=rel&elite=0&mode_device=desktop&identifier=0kW9eGepTT0zZnpXCg",headers=headers,cookies=cookies,params=params)

        print(f"{res.url},Status code : {res.status_code}")

        if res.status_code != 200:
            print(f"Stoping ...Status code : {res.status_code}")
            break
        
        data = res.json()
        items = data.get("items",[])
        if not items:
            print("No more product page!!")
            break
        
        # name = "PL_Page"
        file_path = os.path.join(directory,f"Page_{pages}.json")

        with open(file_path,"w",encoding="utf-8") as f:
            json.dump(data,f,indent=4,ensure_ascii=False)

        
        print(f"Page : {pages},Successfully save json")        
        pages += 1

# save_json_page()

def fetch_data():
    file_path = r"C:\\Users\\mihir.parate\\Desktop\\Python task\\Purplle_web\\Shop_Category\\Skin_care\\LipCare\\Complete_lip_care_collection\\Lipcare_Json_pages"
    
    all_data = []
    for file_name in sorted(os.listdir(file_path)):
        # if file_name.endswith(".json"):
        if file_name.endswith(".json"):
            full_path = os.path.join(file_path,file_name)
            print("\n",file_name)

            with open(full_path,'r',encoding="utf-8") as f:
                data = json.load(f)

            for i in data.get('items',[]):
                product_id = i.get("id")
                product_url = f"https://www.purplle.com/product/{i.get('url')}"

                if product_id and product_url:
                    all_data.append((product_id,product_url))
        
    return all_data

    # print(all_data)
    # for url in all_data:
    #     print(url)

   
fetch_data()

def insert_into_database(db, cursor, data):
    try:
        query = """INSERT INTO Lip_care_PL(Product_id, Product_url, Status) VALUES (%s, %s, 'Pending')"""
        cursor.executemany(query,data)

        db.commit()
        # db.close()
        print("Data Successfully Inserted into Database")

    except Exception as e:
        print("Database Insertion Error:", e)
        
db,cursor = database_creation()
product_details = fetch_data()
if product_details:
    insert_into_database(db,cursor,product_details)

    