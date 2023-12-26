# coding=utf-8
#######################################################################
#
# LICENSE: This license requires that reusers give credit to the creator.
# It allows reusers to distribute, remix, adapt, and build upon the
# material in any medium or format, for noncommercial purposes only. 

# @Title: Shareholding Relationship Graph of Tehran Stock Exchange
# @Author: Mobin Ranjbar
# @Email: mobinranjbar1[at]gmail[dot]com
# @License: CC BY-NC 4.0 (Attribution-NonCommercial 4.0 International)
#
########## Documentation ##############################################
#
# The Tehran Stock Exchange (TSE) is Iran's largest stock exchange, 
# which first opened in 1967. As of May 2023, 666 companies with a
# combined market capitalization of US$1.45 trillion were listed on
# TSE. Iran's capital market has companies from a wide range of
# industries,including automotive, telecommunications, agriculture,
# petrochemical, mining, steel iron, copper, banking and insurance,
# banking and others. Many of the companies listed are state-owned
# firms that have been privatized. (WikiPedia)
#
# Each company that is registered in TSE have their own (private/public)
# shareholders that you can access the information about them publicly
# on TSETMC website. In this project, I used this data to build the
# relationship graph of shareholders using data science and big data
# tools e.g. Apache Spark, MongoDB, Neo4j.
#
# DISCLAIMER: The author of this project did not use any confidential or
# leaked data in the market and It is not responsible for the misuse
# of it. This project has been developed only for research purposes
# and the owner of the data has its rights to request for deletion.
#
# Any sort of contribution is welcome!
#
# Open an issue if you find a bug.
#
# Contact me if you like it.
#
#######################################################################

from neo4j import GraphDatabase
import datetime
import configparser
import pathlib
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import time
import json
import os
os.environ["PYTHONIOENCODING"] = "utf-8"
requests.packages.urllib3.disable_warnings()

start_time = datetime.now()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504,408,104],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

try:
    config = configparser.ConfigParser()
    config_path = pathlib.Path(__file__).parent.absolute() / "../config/config.ini"
    config.read(config_path)
except:
    print("Error: config.ini not found.")
    exit(0)

ids_source = config['DEFAULT']['ids_source']
ids_source_file_path = config['DEFAULT']['ids_source_file_path']

MONGODB_HOST = config['MONGODB']['mongodb_host']
MONGODB_PORT = int(config['MONGODB']['mongodb_port'])
MONGODB_USERNAME = config['MONGODB']['mongodb_username']
MONGODB_PASSWORD = config['MONGODB']['mongodb_password']
MONGODB_DBNAME = config['MONGODB']['mongodb_dbname']
MONGODB_COLLECTION_NAME = config['MONGODB']['mongodb_collection_name']

NEO4J_HOST = config['NEO4J']['neo4j_host']
NEO4J_PORT = int(config['NEO4J']['neo4j_port'])
NEO4J_USERNAME = config['NEO4J']['neo4j_username']
NEO4J_PASSWORD = config['NEO4J']['neo4j_password']

client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT,username=MONGODB_USERNAME,password=MONGODB_PASSWORD)
db = client[MONGODB_DBNAME]
ownership = db[MONGODB_COLLECTION_NAME]
ownership.delete_many({})

headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36'}

last_prices_daily_list_url = "https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{}/0"
shareholder_url = "https://cdn.tsetmc.com/api/Shareholder/{}/{}"

ids_json = []

if ids_source == "file":
    f = open(ids_source_file_path,"r",encoding="utf8")
    ids_json = json.loads(f.read().replace("'","\""))
    f.close()
elif ids_source == "tsetmc":
    url = "https://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx"
    r = http.get(url,headers=headers,verify=False)
    rows = r.text.split("@")[2].split(";")
    for row in rows:
        try:
            fields = row.split(",")
            stock_id = fields[0]
            stock_name = fields[2]
            ids_json.append({"stock_id":stock_id,"stock_name":stock_name})
        except:
            continue
else:
    print("Error: The symbols list source is not defnied.")
    exit(0)

for i,row in enumerate(ids_json):
    shareholders_list = []
    stock_id = row["stock_id"]
    stock_name = row["stock_name"]
    print("Progress: "+str(i+1)+" of "+str(len(ids_json)))
    print("StockId: "+str(stock_id))
    print("Stock Name: "+str(stock_name))
    a = http.get(last_prices_daily_list_url.format(stock_id),headers=headers,verify=False)
    try:
        last_date = str(json.loads(a.text)["closingPriceDaily"][0]["dEven"])
        time.sleep(1)
        b = http.get(shareholder_url.format(stock_id,last_date),headers=headers,verify=False)
        shareholders_result = json.loads(b.text)["shareShareholder"]
        for row in shareholders_result:
            shareholders_list.append({"Sid":row["shareHolderID"],"name":row["shareHolderName"],"stocks":row["numberOfShares"],"percent":row["perOfShares"]})
    except Exception as e:
        print("Error: ",str(e))
        continue
    if len(shareholders_list) != 0:
        shareholders_df = pd.DataFrame(shareholders_list)
        shareholders_df = shareholders_df.drop_duplicates(subset=['Sid'])
        print(shareholders_df)
        row = {"_id":ObjectId(),"stock_id":stock_id,"stock_symbol":stock_name,"shareholders":json.loads(shareholders_df.to_json(orient="records"))}
        ownership.insert_one(row)

    time.sleep(1)

client.close()
end_time = datetime.now()
total_elapsed = end_time - start_time
print("The fetch process which started at "+str(start_time)+" ended at: "+str(end_time)+"\n")
print("Elapsed seconds: "+str(total_elapsed.seconds))
