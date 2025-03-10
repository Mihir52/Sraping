import re
from http.client import responses
from idlelib.browser import file_open
from logging import exception
from math import trunc
from os.path import exists
from pickletools import pyset
from urllib.parse import urlsplit
import requests
import pymysql
import os
import regex
from bs4 import BeautifulSoup
from h11 import PRODUCT_ID
from lxml import html
import pandas as pd
from numpy.f2py.auxfuncs import hasinitvalue
from parsel import Selector
from datetime import datetime
import hashlib
from setuptools.sandbox import save_path
from twisted.conch.insults.window import cursor

cookies = {
    'v2H': 't',
    'uts': '1740134907884',
    'session_id': '1e724895-55b9-40cc-994c-7f5428903cc5',
    'dtCookie': 'v_4_srv_22_sn_913C2CF2314A73D7128DB84DB161F6C8_perc_100000_ol_0_mul_1_app-3A0eed2717dafcc06d_1_rcs-3Acss_0',
    'XSRF-TOKEN': 'eXD8S04ljxlrFg==.OmJMrx0Oboh+Pn5FcYvt2vWAUn4E/74tZkmydSr52xs=',
    'at_check': 'true',
    'AMCVS_5E16123F5245B2970A490D45%40AdobeOrg': '1',
    'USER_LOC': 'eyJsYSI6IjIzLjAzIiwibG8iOiI3Mi42MiJ9',
    'akacd_homepage_v2S': '1747910917~rv=23~id=456f81daa2985112b1769aa1387b1ad7',
    'kampyle_userid': '7f61-4e34-a9a7-36f0-5049-d5a5-3e9f-51f9',
    'nos': 'BdhdLYK4gb',
    'Tld-kampyleUserSession': '1740135077604',
    'Tld-kampyleUserSessionsCount': '2',
    'Tld-kampyleUserPercentile': '4.074418175474359',
    'alg_idx_qry': '{"indexNames":["productSku"],"queryIds":["d8f45a0e82560bae4094578748d0f5cb"]}',
    'akacd_www_pdp': '1747911168~rv=44~id=1a4fc6cdd87e044aeb5eeaac8ba56513',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Fri+Feb+21+2025+16%3A22%3A53+GMT%2B0530+(India+Standard+Time)&version=202403.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=52468fef-432f-451b-bf40-88bfbc7fe9aa&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0007%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0005%3A1&AwaitingReconsent=false',
    'rvi': 'prod6145594',
    'AMCV_5E16123F5245B2970A490D45%40AdobeOrg': '179643557%7CMCIDTS%7C20141%7CMCMID%7C07701997182075605892217607122276003972%7CMCOPTOUT-1740142376s%7CNONE%7CvVersion%7C5.5.0',
    'Tld-kampyleSessionPageCounter': '3',
    'Tld-kampyleInvitePresented': 'true',
    'Tld-LAST_INVITATION_VIEW': '1740135184384',
    'Tld-DECLINED_DATE': '1740135554717',
    '_abck': '3D333BF807AE077ECACD62127C967D18~-1~YAAQLHLBF7WU40mVAQAAIhE8fw2Agnc3CY+decGdWtwHKtLeRsCMV+RHC4XPhwof3ccgraX7WY0yi48/dnCR+S1tLQ7TkWhV1Kqn+XolY8Ppjjg+Bmk+uSPz4+AQyzSPCc/w0dRu427djMhflAdGH8h5gp8S+6Jgkxug0hqDBjN565iYcMBKdsgj/cbJDv6bPZgPlINL+pZGLxOdPnbEgyj9Fwxsua5m6VPWisRKY8PFsvprf8p2RDG3Iwo1kJQlWlh+WEcdlK4k6j8j/wiLeYsw1K1wK+Y25Gn1k9bed7h6p8k76OcIAPZMCM++giGZPySzgBuUzIUDxRVsttKWc0DIBjPKbT9vxdkNzQK+mANLkbe9ZxQFPR0RSSf7dytIct35c5Sbrm3zFmztMKYxqOk802spLTJJWguObi97kUJuUjfIJ28MTWv5e7cDPl1U92p4nB4MAAK242C/VFVqYvdsG2bk1vqWjBkguuQ0CU36CeAeqrSML22K61/V7BO8F72pBEVQ8lsrIybYqQnOfSb4cIAndJLYjPBJxO9E4LVa6PX1LJs7t9ElrRIHOAnKo6kJVmFQitCdTd7FAlRWKGYMoVUhfX5Wvt1qRwITgUAXAfdub/LD/5X9qHpIFHtjfiuF9z1klxo69OcEdDVzFT97eTJRp4n2X6TGBiWSxtSqG20JTM12ReunqZ0FpHGMSN71JAkCHYDeLNMZHQ0sK/n9MciVuzvdTJb4C5d0moQa3PGmZ78=~-1~-1~-1',
    'AKA_A2': 'A',
    'ak_bmsc': 'BEFC23CD6A419AF417DF3F93387CB74E~000000000000000000000000000000~YAAQLHLBFzDbAUqVAQAASuCyfxuq8WF18k33glDjYcAhvwcqzXJ8LM9BOxOGvQbyT0TOPQ6MDoE3q04btWSD3Yefpb2PFkvwVyqe12jEwhcDeSl7Hoz9ncRaiBn+CF+7uSZu/EU5IGHvTR6NEow8uKV0yk36h70bZ0N/Pi+H30uCVtH0umIDkLgmmwKNJzblNWp9oEDG4oS9NY8W5DAGhNhdobNNqayXjDwNlbLLjqs7YAIZuyda81ZRWNQ/5CRUgxmq0KLnb2q3IX5mzUbIEZB3UFO1dTVWIafsp+QCVDZozozFKzeyXsOTfVZktZmOfJ28B6+EBFtbczT6TDzXGNAFuH+FK7PUr8KWomWjxN4KjnZhBFnCeSLMJ/N0M4+muuZkqk2eM1Eddevq',
    'bm_sv': 'DB1EB8D28449A4523BA1F75706216118~YAAQTHLBFwtggVSVAQAAPSy9fxsaag1WIk4bcV3lrkycKPKok7Rh4NTWmNIW5Tz623I3ugGqiCLHzU4pvFKaJCWU3BoIpvPFzpsbQyRCODKMhw0Vo6RcrkhfK6jo9GrAlQFl2jwU8w38ZvSj/hFtG1iyCkm6Yl/vAjNHytIzW+xD7jkTWEtE2WxvE4c2ud2b3+k5QXDaLxrRlbt0NeJYTNtuy/bwWXKcRVaF8+JBjASZY1KTPAHxMLm5q7oaLkpQQC5e~1',
    'bm_sz': 'E2BDA6DFAA401E0D218306F40BD45D4A~YAAQTHLBFwxggVSVAQAAPSy9fxvA6i5QTk7GeVJGMmyNde7Mka99a7eGr8GqG0AC5mTL0GC87BPILyd4y242bwNodEv9YLVTalD1otask4npRkBNdGYAgX9kF9BMCvk0QPTHJqza7KtudoVT85HeJL6B6uwZrZBXf2mKDJBwrHnroPnvcndEWmdO+Hw4HcnrkPfKz738eqCe5MXGczT3OQ6QU3myZO/55nUz6Hi0p+Du9ioHZssEJgUt5EthGtRaldW1Z8LfYQOHvl01J39+Wm8g0lOz2dpAUrFRnK586xAw238Jouzmj6IGMoSWar/MKIScDV8VykaZD9/Yyig9RLl/lAagQoUE0Wv2WWagfw6GWRIPAmcQTZMDOBs9cdL6xfUJXD/TgFwBs2BDBPFN3QXUmNy/N2w5ultM+XbAk/4X+DTczeN/cf2uoyO4qGWeAzFrW8aFvQ==~4339267~3621698',
    'akavpau_walgreens': '1741605159~id=d8c35717ca2d467481222613cae9b6e7',
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
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    # 'cookie': 'v2H=t; uts=1740134907884; session_id=1e724895-55b9-40cc-994c-7f5428903cc5; dtCookie=v_4_srv_22_sn_913C2CF2314A73D7128DB84DB161F6C8_perc_100000_ol_0_mul_1_app-3A0eed2717dafcc06d_1_rcs-3Acss_0; XSRF-TOKEN=eXD8S04ljxlrFg==.OmJMrx0Oboh+Pn5FcYvt2vWAUn4E/74tZkmydSr52xs=; at_check=true; AMCVS_5E16123F5245B2970A490D45%40AdobeOrg=1; USER_LOC=eyJsYSI6IjIzLjAzIiwibG8iOiI3Mi42MiJ9; akacd_homepage_v2S=1747910917~rv=23~id=456f81daa2985112b1769aa1387b1ad7; kampyle_userid=7f61-4e34-a9a7-36f0-5049-d5a5-3e9f-51f9; nos=BdhdLYK4gb; Tld-kampyleUserSession=1740135077604; Tld-kampyleUserSessionsCount=2; Tld-kampyleUserPercentile=4.074418175474359; alg_idx_qry={"indexNames":["productSku"],"queryIds":["d8f45a0e82560bae4094578748d0f5cb"]}; akacd_www_pdp=1747911168~rv=44~id=1a4fc6cdd87e044aeb5eeaac8ba56513; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Feb+21+2025+16%3A22%3A53+GMT%2B0530+(India+Standard+Time)&version=202403.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=52468fef-432f-451b-bf40-88bfbc7fe9aa&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0007%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0005%3A1&AwaitingReconsent=false; rvi=prod6145594; AMCV_5E16123F5245B2970A490D45%40AdobeOrg=179643557%7CMCIDTS%7C20141%7CMCMID%7C07701997182075605892217607122276003972%7CMCOPTOUT-1740142376s%7CNONE%7CvVersion%7C5.5.0; Tld-kampyleSessionPageCounter=3; Tld-kampyleInvitePresented=true; Tld-LAST_INVITATION_VIEW=1740135184384; Tld-DECLINED_DATE=1740135554717; _abck=3D333BF807AE077ECACD62127C967D18~-1~YAAQLHLBF7WU40mVAQAAIhE8fw2Agnc3CY+decGdWtwHKtLeRsCMV+RHC4XPhwof3ccgraX7WY0yi48/dnCR+S1tLQ7TkWhV1Kqn+XolY8Ppjjg+Bmk+uSPz4+AQyzSPCc/w0dRu427djMhflAdGH8h5gp8S+6Jgkxug0hqDBjN565iYcMBKdsgj/cbJDv6bPZgPlINL+pZGLxOdPnbEgyj9Fwxsua5m6VPWisRKY8PFsvprf8p2RDG3Iwo1kJQlWlh+WEcdlK4k6j8j/wiLeYsw1K1wK+Y25Gn1k9bed7h6p8k76OcIAPZMCM++giGZPySzgBuUzIUDxRVsttKWc0DIBjPKbT9vxdkNzQK+mANLkbe9ZxQFPR0RSSf7dytIct35c5Sbrm3zFmztMKYxqOk802spLTJJWguObi97kUJuUjfIJ28MTWv5e7cDPl1U92p4nB4MAAK242C/VFVqYvdsG2bk1vqWjBkguuQ0CU36CeAeqrSML22K61/V7BO8F72pBEVQ8lsrIybYqQnOfSb4cIAndJLYjPBJxO9E4LVa6PX1LJs7t9ElrRIHOAnKo6kJVmFQitCdTd7FAlRWKGYMoVUhfX5Wvt1qRwITgUAXAfdub/LD/5X9qHpIFHtjfiuF9z1klxo69OcEdDVzFT97eTJRp4n2X6TGBiWSxtSqG20JTM12ReunqZ0FpHGMSN71JAkCHYDeLNMZHQ0sK/n9MciVuzvdTJb4C5d0moQa3PGmZ78=~-1~-1~-1; AKA_A2=A; ak_bmsc=BEFC23CD6A419AF417DF3F93387CB74E~000000000000000000000000000000~YAAQLHLBFzDbAUqVAQAASuCyfxuq8WF18k33glDjYcAhvwcqzXJ8LM9BOxOGvQbyT0TOPQ6MDoE3q04btWSD3Yefpb2PFkvwVyqe12jEwhcDeSl7Hoz9ncRaiBn+CF+7uSZu/EU5IGHvTR6NEow8uKV0yk36h70bZ0N/Pi+H30uCVtH0umIDkLgmmwKNJzblNWp9oEDG4oS9NY8W5DAGhNhdobNNqayXjDwNlbLLjqs7YAIZuyda81ZRWNQ/5CRUgxmq0KLnb2q3IX5mzUbIEZB3UFO1dTVWIafsp+QCVDZozozFKzeyXsOTfVZktZmOfJ28B6+EBFtbczT6TDzXGNAFuH+FK7PUr8KWomWjxN4KjnZhBFnCeSLMJ/N0M4+muuZkqk2eM1Eddevq; bm_sv=DB1EB8D28449A4523BA1F75706216118~YAAQTHLBFwtggVSVAQAAPSy9fxsaag1WIk4bcV3lrkycKPKok7Rh4NTWmNIW5Tz623I3ugGqiCLHzU4pvFKaJCWU3BoIpvPFzpsbQyRCODKMhw0Vo6RcrkhfK6jo9GrAlQFl2jwU8w38ZvSj/hFtG1iyCkm6Yl/vAjNHytIzW+xD7jkTWEtE2WxvE4c2ud2b3+k5QXDaLxrRlbt0NeJYTNtuy/bwWXKcRVaF8+JBjASZY1KTPAHxMLm5q7oaLkpQQC5e~1; bm_sz=E2BDA6DFAA401E0D218306F40BD45D4A~YAAQTHLBFwxggVSVAQAAPSy9fxvA6i5QTk7GeVJGMmyNde7Mka99a7eGr8GqG0AC5mTL0GC87BPILyd4y242bwNodEv9YLVTalD1otask4npRkBNdGYAgX9kF9BMCvk0QPTHJqza7KtudoVT85HeJL6B6uwZrZBXf2mKDJBwrHnroPnvcndEWmdO+Hw4HcnrkPfKz738eqCe5MXGczT3OQ6QU3myZO/55nUz6Hi0p+Du9ioHZssEJgUt5EthGtRaldW1Z8LfYQOHvl01J39+Wm8g0lOz2dpAUrFRnK586xAw238Jouzmj6IGMoSWar/MKIScDV8VykaZD9/Yyig9RLl/lAagQoUE0Wv2WWagfw6GWRIPAmcQTZMDOBs9cdL6xfUJXD/TgFwBs2BDBPFN3QXUmNy/N2w5ultM+XbAk/4X+DTczeN/cf2uoyO4qGWeAzFrW8aFvQ==~4339267~3621698; akavpau_walgreens=1741605159~id=d8c35717ca2d467481222613cae9b6e7',
}


def html_page_save():
    response = requests.get('https://www.walgreens.com/sitemap-index.xml', cookies=cookies, headers=headers)
    try:
        if response.status_code == 200:
            save_path = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Wallgreens_Sitemap\\Main_html_page"
            os.makedirs(os.path.dirname(save_path),exist_ok=True)

            with open(save_path,'w',encoding='utf-8') as f:
                f.write(response.text)
            print(f"Page succesfull save : {save_path}")
        else:
            print(f"Status code : {response.status_code}")
    except Exception as e:
        print(f"**html page save : {e}")

# html_page_save()

def read_html_file(file_path):
    try:
        with open(file_path,"r",encoding='utf-8') as f:
            content = f.read()
        urls = re.findall(r'<loc>(.*?)</loc>',content)
        # print(len(urls))
        return urls
    except Exception as e:
        print(f"**file read error : {e}")

# read_html_file(file_path="Main_html_page")

def insert_into_db(urls):
    try:
        now = datetime.now()
        date_time = now.strftime("%y%m%d__%H%M%S")
        day = now.strftime("%A")
        db_name = f"walgreens_{date_time}_{day}"

        db = pymysql.connect(host="localhost",user="root",password="xbyte")
        cursor = db.cursor()

        # cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        # db.commit()
        print(f"Databse create succesfully : {db_name}")
    except Exception as e:
        print(f"**Dabase create error : {e}")

    try:
        db = pymysql.connect(host="localhost",user="root",password="xbyte",database=db_name)
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS main_page_urls(
                ID INT NOT NULL AUTO_INCREMENT,
                URLS TEXT,
                Status VARCHAR(50) DEFAULT "Pending",
                PRIMARY KEY (ID))
            """)
        db.commit()
    except Exception as e:
        print(f"***Table error : {e}")

    try:
        # for u in urls:
        #     cursor.execute("INSERT INTO main_page_urls (URLS,Status) VALUES (%s,'done')",(u,))
        # print("Data insert succesfully")

        db.commit()
        db.close()
    except Exception as e:
        print(f"**Insertion error : {e}")

def save_to_excel(urls):
    try:
        df = pd.DataFrame(urls, columns=["URL"])
        excel_path = "C:\\Users\\mihir.parate\\Desktop\\Python task\\Wallgreens_Sitemap\\Main_page_urls.xlsx"
        df.to_excel(excel_path, index=False)
        print(f"Data saved in Excel: {excel_path}")
    except Exception as e:
        print(f"**Excel error: {e}")

# read_html_file(file_path="Main_html_page")
urls = read_html_file(file_path="Main_html_page")
if urls:
    insert_into_db(urls)
    save_to_excel(urls)












