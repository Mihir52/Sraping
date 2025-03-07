import json
import os
import requests
import pymysql
import requests


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
    '_gat_global': '1',
    '_gat_country': '1',
    '_ga_X6B66E85ZJ': 'GS1.2.1741352995.4.0.1741352995.60.0.0',
    '_ga_2XVFHLPTVP': 'GS1.1.1741352930.4.1.1741352995.60.0.0',
    '_dd_s': 'rum=0&expire=1741353895919',
    'AWSALBTG': 'MRt270cbILMecZbJLJNYUx9tm5zA8/Qz6lCgDtZwV0rTJ/Z+mMY3VeIqqA7LKeH+wJrvUgYQB6q3LZP2Sq3vmx+2fig1FHwpvpGG0NeOoulDQzUqV7H8vTW+PmzRbMh2mPEaR4eeHLYcnFZrzYPy710Pdh5jhnlSpdWb9jr9JyRX',
    'AWSALBTGCORS': 'MRt270cbILMecZbJLJNYUx9tm5zA8/Qz6lCgDtZwV0rTJ/Z+mMY3VeIqqA7LKeH+wJrvUgYQB6q3LZP2Sq3vmx+2fig1FHwpvpGG0NeOoulDQzUqV7H8vTW+PmzRbMh2mPEaR4eeHLYcnFZrzYPy710Pdh5jhnlSpdWb9jr9JyRX',
    '_ga': 'GA1.2.133479830.1741340340',
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
    # 'cookie': 'PHPSESSID=efb1042c1d9f827ec410220177f3f51a; csrf=3a814420ec37beda1ff0bc593f8e8aa3; fbcity=11722; fre=0; rd=1380000; zl=en; fbtrack=e370c80a2a04f6b58ce5f7e780211777; ltv=173508; lty=173508; locus=%7B%22addressId%22%3A0%2C%22lat%22%3A10.77052%2C%22lng%22%3A76.39898%2C%22cityId%22%3A11722%2C%22ltv%22%3A173508%2C%22lty%22%3A%22subzone%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A79852%7D; _gid=GA1.2.997639609.1741340340; _gcl_au=1.1.214743333.1741340341; _fbp=fb.1.1741340343378.842193404776764524; _ga_DYN7ZGYTHM=GS1.2.1741342240.1.0.1741342240.0.0.0; _ga_L7TYXFHY80=GS1.2.1741342240.1.0.1741342240.0.0.0; ak_bmsc=FDBD740FCCB95D5F2E90DA5D66185E13~000000000000000000000000000000~YAAQLXLBF17bWUiVAQAA70CccBtUDJELozIx5KiyJMIamr26YYSa2OFw3vhsQ4XVL/EIU239h+o1yCwtjFrrwU6bOVHFsGealYt/hYAYfL/FDSF6fQSxhYMc+yAZD9CPNlgf6uNR3UR1li5g4Yde4XPIWSFDZNudWL1se4bn6mIr0pV46J+PLw7moKok1lup2s3jYOgRz8j1oA1in24nHK4LaFh+HlU7OrG2qHcz2U946+4zBxm1RdoHgXGpINEte75yqXE06WLHvz3CUJxO6OFlZRr0mW7VzWMk5DsbZLbM4r60GLhLuO1HemiT/5DRCciDR2nuPUvPZkf+UpagGX2KTlQ0CbwTUib1u1oFXHmfutYQLlVuno0JML0te4NRFoIvbbq8aZeE; _gat_global=1; _gat_country=1; _ga_X6B66E85ZJ=GS1.2.1741352995.4.0.1741352995.60.0.0; _ga_2XVFHLPTVP=GS1.1.1741352930.4.1.1741352995.60.0.0; _dd_s=rum=0&expire=1741353895919; AWSALBTG=MRt270cbILMecZbJLJNYUx9tm5zA8/Qz6lCgDtZwV0rTJ/Z+mMY3VeIqqA7LKeH+wJrvUgYQB6q3LZP2Sq3vmx+2fig1FHwpvpGG0NeOoulDQzUqV7H8vTW+PmzRbMh2mPEaR4eeHLYcnFZrzYPy710Pdh5jhnlSpdWb9jr9JyRX; AWSALBTGCORS=MRt270cbILMecZbJLJNYUx9tm5zA8/Qz6lCgDtZwV0rTJ/Z+mMY3VeIqqA7LKeH+wJrvUgYQB6q3LZP2Sq3vmx+2fig1FHwpvpGG0NeOoulDQzUqV7H8vTW+PmzRbMh2mPEaR4eeHLYcnFZrzYPy710Pdh5jhnlSpdWb9jr9JyRX; _ga=GA1.2.133479830.1741340340',
}


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
            CREATE TABLE IF NOT EXISTS Zomato_Pl(
                ID INT NOT NULL AUTO_INCREMENT,
                Product_url TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID)
            )
        """)
        db.commit()
    except Exception as e:
        print("Table creation****", e)

    return db, cursor

def save_json_page():
    directory = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Mock_test_3\\Shop_Category\\Skin_care\\LipCare\\Complete_lip_care_collection\\Lipcare_Json_pages"
    os.makedirs(directory, exist_ok=True)

    pages = 1

    while pages:
        # api_url = "https://www.purplle.com/neo/merch/items/v2?tenant=PURPLLE_COM&sub_tenant=MAIN_SITE&userType=0&list_type=category&list_type_value=skincare/lip-care&page=1&sort_by=rel&elite=0&mode_device=desktop&identifier=0kW9eGepTT0zZnpXCg"

        res = requests.get(
            f"https://www.purplle.com/neo/merch/items/v2?tenant=PURPLLE_COM&sub_tenant=MAIN_SITE&userType=0&list_type=category&list_type_value=skincare/lip-care&page={pages}&sort_by=rel&elite=0&mode_device=desktop&identifier=0kW9eGepTT0zZnpXCg",
            headers=headers, cookies=cookies, params=params)

        print(f"{res.url},Status code : {res.status_code}")

        if res.status_code != 200:
            print(f"Stoping ...Status code : {res.status_code}")
            break

        data = res.json()
        items = data.get("items", [])
        if not items:
            print("No more product page!!")
            break

        # name = "PL_Page"
        file_path = os.path.join(directory, f"Page_{pages}.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Page : {pages},Successfully save json")
        pages += 1


# save_json_page()

def fetch_data():
    file_path = r"C:\\Users\\mihir.parate\\Desktop\\Mock_Test-3\\Zomato"

    all_data = []
    # for file_name in sorted(os.listdir(file_path)):
    for file_name in os.listdir(file_path):
        if file_name.endswith(".json"):
            full_path = os.path.join(file_path, file_name)
            print("\n", file_name)

            with open(full_path, 'r', encoding="utf-8") as f:
                data = json.load(f)

            for i in data.get('itemListElement', []):
                product_url = f"https://www.zomato.com{i.get("item", {}).get("url")}"
                if product_url:
                    all_data.append(product_url)
    return all_data

    # print(all_data)
    # for url in all_data:
    #     print(url)


fetch_data()


def insert_into_database(db, cursor, data):
    try:
        query = """INSERT INTO Zomato_Pl(Product_url, Status) VALUES (%s, 'Pending')"""
        cursor.executemany(query, data)

        db.commit()
        # db.close()
        print("Data Successfully Inserted into Database")

    except Exception as e:
        print("Database Insertion Error:", e)


db, cursor = database_creation()
product_details = fetch_data()
if product_details:
    insert_into_database(db, cursor, product_details)

